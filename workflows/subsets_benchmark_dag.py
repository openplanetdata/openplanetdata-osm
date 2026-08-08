"""
OSM Subsets Benchmark DAG - Phase 0 calibration for the subset pipeline.

Schedule: manual trigger only.

Runs the full subset pipeline (gol query -> PBF, gol build -> GOL, gol save ->
GOB, DuckDB -> GeoParquet) for one continent (europe), one country with
overseas territories (FR) and one region (FR-IDF). Each subset step is its own
Airflow task (prepare boundary / build files / geoparquet per subset), so
progress and per-step wall times are visible directly in the UI and a crash
loses at most one step's log. The final report task aggregates the timings
into a summary. Use the numbers to calibrate the daily/weekly cadence of the
subsets DAGs.

Also verifies extract semantics:
- the FR PBF includes overseas territories (Reunion bbox must match features)
- the subset PBF rebuilds into a GOL (gol >= 2.3.2 rejects ways with missing
  nodes, so a successful build proves boundary-crossing ways are complete)

Outputs are kept in /data/openplanetdata/osm/subsets/benchmark for manual
inspection; remove the directory when done.
"""

import os
import sys
import time
from datetime import timedelta
from pathlib import Path

from airflow.sdk import DAG, task
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.defaults import (
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_GOL_PATH,
    SHARED_PLANET_OSM_PARQUET_PATH,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/subsets/benchmark"
BOUNDARIES_DIR = f"{WORK_DIR}/boundaries"
SNAPSHOT_GOL = f"{WORK_DIR}/planet-latest.osm.gol"
SNAPSHOT_PARQUET = f"{WORK_DIR}/planet-latest.osm.parquet"

# (code, level, R2 boundary path, boundary filename)
BENCHMARK_SUBSETS = [
    ("europe", "continents", "boundaries/continents/europe/geojson", "europe-latest.boundary.geojson"),
    ("FR", "countries", "boundaries/countries/FR/geojson", "FR-latest.boundary.geojson"),
    ("FR-IDF", "regions", "boundaries/regions/FR-IDF/geojson", "FR-IDF-latest.boundary.geojson"),
]

# Reunion island: proof that the FR extract includes overseas territories.
REUNION_BBOX = (55.2, -21.4, 55.9, -20.8)


def _utils():
    """Import workflows.utils.osm_subsets at task runtime (bundle-relative)."""
    bundle_root = str(Path(__file__).resolve().parent.parent)
    if bundle_root not in sys.path:
        sys.path.insert(0, bundle_root)
    from workflows.utils import osm_subsets

    return osm_subsets


def _print_sizes(code: str, level: str) -> None:
    subset_dir = f"{WORK_DIR}/{level}/{code}"
    print(f"[{code}] output sizes:")
    for entry in sorted(os.listdir(subset_dir)):
        size = os.path.getsize(f"{subset_dir}/{entry}")
        print(f"  {entry}: {size / 1024**3:,.2f} GiB")


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData OSM Subsets Benchmark",
    dag_id="openplanetdata_osm_subsets_benchmark",
    default_args={
        "execution_timeout": timedelta(hours=8),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_osm",
        "priority_weight": 1,
        "queue": "cortex",
        "retries": 0,
        "weight_rule": "absolute",
    },
    description="Phase 0 calibration benchmark for the OSM subset pipeline",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=1,
    schedule=None,
    tags=["benchmark", "openplanetdata", "osm", "subsets"],
) as dag:

    @task(task_display_name="Snapshot Planet Inputs")
    def snapshot_inputs() -> None:
        """Hardlink the shared planet GOL and GeoParquet for a stable snapshot."""
        from airflow.exceptions import AirflowException

        os.makedirs(WORK_DIR, exist_ok=True)
        for source, snapshot in [
            (SHARED_PLANET_OSM_GOL_PATH, SNAPSHOT_GOL),
            (SHARED_PLANET_OSM_PARQUET_PATH, SNAPSHOT_PARQUET),
        ]:
            if not os.path.exists(source):
                raise AirflowException(
                    f"Missing shared planet input: {source} - run the planet DAGs first"
                )
            if os.path.exists(snapshot):
                os.remove(snapshot)
            os.link(source, snapshot)

    @task.r2index_download(
        task_display_name="Download Benchmark Boundaries",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_boundary(source_path: str, filename: str, code: str) -> DownloadItem:
        """Download one per-entity boundary GeoJSON from R2."""
        return DownloadItem(
            destination=f"{BOUNDARIES_DIR}/{code}.raw.geojson",
            source_filename=filename,
            source_path=source_path,
            source_version="v2",
        )

    @task(task_display_name="Install DuckDB")
    def install_duckdb() -> None:
        """Download the DuckDB CLI once into the work directory."""
        subsets = _utils()
        script = "set -euo pipefail\n" + subsets.INSTALL_DUCKDB_TEMPLATE.format(work_dir=WORK_DIR)
        subsets.run_in_container(script)

    @task(task_display_name="Normalize Boundaries")
    def normalize_boundaries() -> None:
        """Rewrite downloaded boundaries with the layer name the simplify SQL expects."""
        import json

        for code, _level, _path, _filename in BENCHMARK_SUBSETS:
            raw_path = f"{BOUNDARIES_DIR}/{code}.raw.geojson"
            with open(raw_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if data.get("type") == "Feature":
                data = {"type": "FeatureCollection", "features": [data]}
            data["name"] = "boundary"
            with open(raw_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh)

    @task(task_display_name="Reset Previous Outputs")
    def reset_outputs() -> None:
        """Remove outputs of previous benchmark runs.

        The pipeline helpers skip existing outputs, which would turn re-run
        timings into no-ops.
        """
        import shutil

        for code, level, _path, _filename in BENCHMARK_SUBSETS:
            shutil.rmtree(f"{WORK_DIR}/{level}/{code}", ignore_errors=True)
            for leftover in (f"{WORK_DIR}/{level}/{code}.done", f"{BOUNDARIES_DIR}/{code}.meta.json",
                             f"{BOUNDARIES_DIR}/{code}.prepared.geojson"):
                if os.path.exists(leftover):
                    os.remove(leftover)

    @task
    def prepare_boundary(code: str) -> dict:
        """Buffer + simplify one boundary; fail loudly if preparation fails."""
        from airflow.exceptions import AirflowException

        subsets = _utils()
        start = time.monotonic()
        failure = subsets.prepare_boundary(code, BOUNDARIES_DIR)
        elapsed = time.monotonic() - start
        print(f"[{code}] prepare boundary: {elapsed:,.1f}s")
        if failure is not None:
            raise AirflowException(f"[{code}] boundary preparation failed")
        return {"code": code, "step": "prepare boundary", "elapsed": elapsed}

    @task
    def build_files(code: str, level: str) -> dict:
        """gol query -> PBF, gol build -> GOL, gol save -> GOB for one subset."""
        from airflow.exceptions import AirflowException

        subsets = _utils()
        level_dir = f"{WORK_DIR}/{level}"
        os.makedirs(level_dir, exist_ok=True)
        start = time.monotonic()
        result = subsets.build_subset_files(code, level_dir, BOUNDARIES_DIR, SNAPSHOT_GOL)
        elapsed = time.monotonic() - start
        print(f"[{code}] pbf + gol + gob: {elapsed:,.1f}s")
        if result is not None:
            raise AirflowException(f"[{code}] pipeline failed: {result}")
        _print_sizes(code, level)
        return {"code": code, "step": "pbf + gol + gob", "elapsed": elapsed}

    @task
    def build_geoparquet(code: str, level: str) -> dict:
        """Extract one subset GeoParquet from the planet GeoParquet snapshot."""
        from airflow.exceptions import AirflowException

        subsets = _utils()
        level_dir = f"{WORK_DIR}/{level}"
        start = time.monotonic()
        failed = subsets.run_parquet_batch([code], level_dir, BOUNDARIES_DIR, SNAPSHOT_PARQUET, WORK_DIR)
        elapsed = time.monotonic() - start
        print(f"[{code}] geoparquet: {elapsed:,.1f}s")
        if failed:
            raise AirflowException(f"[{code}] parquet extraction failed")
        _print_sizes(code, level)
        return {"code": code, "step": "geoparquet", "elapsed": elapsed}

    @task(task_display_name="Verify Semantics & Report")
    def verify_and_report(timings: list[dict]) -> None:
        """Check overseas-territory semantics on FR and print the timing summary."""
        from airflow.exceptions import AirflowException

        subsets = _utils()

        # Semantics check: FR must include Reunion (overseas territory).
        minx, miny, maxx, maxy = REUNION_BBOX
        fr_parquet = f"{WORK_DIR}/countries/FR/FR-latest.osm.parquet"
        out = subsets.run_in_container(
            f"{WORK_DIR}/duckdb -csv -noheader -c \"SELECT count(*) FROM '{fr_parquet}' "
            f"WHERE bbox.xmax >= {minx} AND bbox.xmin <= {maxx} "
            f"AND bbox.ymax >= {miny} AND bbox.ymin <= {maxy}\"",
            env={"HOME": WORK_DIR},
            stdout_only=True,
        )
        reunion_count = int(out.decode().strip())
        print(f"[FR] features in Reunion bbox (parquet): {reunion_count}")
        if reunion_count == 0:
            raise AirflowException("FR parquet subset is missing Reunion - overseas territories dropped")

        # CSV prints one line per feature plus a header line, so more than one
        # line proves features exist. (An empty GeoJSON result still contains
        # the FeatureCollection wrapper, so substring checks on it are
        # vacuously true - do not use GeoJSON here.)
        fr_gol = f"{WORK_DIR}/countries/FR/FR-latest.osm.gol"
        out = subsets.run_in_container(
            f"gol query {fr_gol} '*' -b {minx},{miny},{maxx},{maxy} -f csv | head -1000 | wc -l",
            stdout_only=True,
        )
        gol_lines = int(out.decode().strip())
        print(f"[FR] csv lines for Reunion bbox (gol): {gol_lines}")
        if gol_lines <= 1:
            raise AirflowException("FR GOL subset is missing Reunion - overseas territories dropped")
        print("[FR] Reunion present in subset GOL")

        print("\n=== Benchmark summary ===")
        for timing in timings:
            print(f"{timing['code']:10s} {timing['step']:20s} {timing['elapsed']:10,.1f}s")
        print(f"\nOutputs kept in {WORK_DIR} for inspection - remove manually when done.")

    # Task flow: one prepare/build/geoparquet chain per subset, run
    # sequentially (max_active_tasks=1) so timings never overlap.
    snapshot = snapshot_inputs()
    downloads = [
        download_boundary.override(task_id=f"download_boundary_{code.lower().replace('-', '_')}")(
            source_path=path, filename=filename, code=code,
        )
        for code, _level, path, filename in BENCHMARK_SUBSETS
    ]
    normalized = normalize_boundaries()
    duckdb_install = install_duckdb()
    reset = reset_outputs()

    snapshot >> downloads
    downloads >> normalized >> duckdb_install >> reset

    timings = []
    previous = reset
    for code, level, _path, _filename in BENCHMARK_SUBSETS:
        slug = code.lower().replace("-", "_")
        prepared = prepare_boundary.override(
            task_id=f"prepare_boundary_{slug}",
            task_display_name=f"Prepare Boundary [{code}]",
        )(code)
        built = build_files.override(
            task_id=f"build_files_{slug}",
            task_display_name=f"Build PBF/GOL/GOB [{code}]",
        )(code, level)
        parquet = build_geoparquet.override(
            task_id=f"build_geoparquet_{slug}",
            task_display_name=f"Build GeoParquet [{code}]",
        )(code, level)
        previous >> prepared >> built >> parquet
        timings += [prepared, built, parquet]
        previous = parquet

    verify_and_report(timings=timings)
