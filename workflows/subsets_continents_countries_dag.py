"""
OSM Subsets Continents & Countries DAG - extracts in PBF, GeoParquet, GOL and GOB.

Schedule: Triggered when BOTH the planet GOL v2 and planet GeoParquet shared
copies have been refreshed (same-day snapshot consistency) - i.e. it follows
the planet pipeline's cadence, currently daily.

Pipeline per subset (see workflows/utils/osm_subsets.py):
1. gol query --area <boundary> -f pbf against a snapshot of the planet GOL
2. gol build + gol save for the subset GOL/GOB
3. DuckDB COPY from a snapshot of the planet GeoParquet (bbox pruning + ST_Intersects)
4. Upload all four formats to R2

Scheduling policy: every task runs in the shared openplanetdata_osm pool with
an absolute priority weight of 1, far below the planet DAGs (their
OldestFirstPriorityStrategy weight is the run age in seconds), so whenever a
planet task and a subset task are both queued, the planet task always takes
the free slot first. Airflow never preempts a running task, so a planet task
can still wait behind an in-flight subset batch; batches are deliberately
small so that wait is bounded by a single batch, not the whole run.
"""

import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path

from airflow.sdk import DAG, Asset, task
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.data.continents import CONTINENTS
from openplanetdata.airflow.data.countries import COUNTRIES
from openplanetdata.airflow.defaults import (
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_GOL_PATH,
    SHARED_PLANET_OSM_PARQUET_PATH,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/subsets/continents-countries"
BOUNDARIES_DIR = f"{WORK_DIR}/boundaries"
SNAPSHOT_GOL = f"{WORK_DIR}/planet-latest.osm.gol"
SNAPSHOT_PARQUET = f"{WORK_DIR}/planet-latest.osm.parquet"

CONTINENTS_AGGREGATE = f"{WORK_DIR}/planet-latest.continents.geojson"
COUNTRIES_AGGREGATE = f"{WORK_DIR}/planet-latest.countries.geojson"

# Continents are planet-scale extracts: one per batch, so the pool slot is
# yielded after every single continent and a queued planet task never waits
# behind more than one continent's work. Countries are far smaller.
CONTINENT_BATCH_SIZE = 1
COUNTRY_BATCH_SIZE = 32
BUILD_WORKERS = 2

# Both assets are emitted by the planet DAGs' copy_to_shared tasks, so a
# trigger guarantees the shared files this DAG snapshots actually exist.
GOL_V2_ASSET = Asset(
    name="openplanetdata-osm-planet-gol-v2",
    uri=f"s3://{R2_BUCKET}/osm/planet/gol/v2/planet-latest.osm.gol",
)
GEOPARQUET_SHARED_ASSET = Asset(
    name="openplanetdata-osm-planet-geoparquet-shared",
    uri=f"file://{SHARED_PLANET_OSM_PARQUET_PATH}",
)


def _utils():
    """Import workflows.utils.osm_subsets at task runtime (bundle-relative)."""
    bundle_root = str(Path(__file__).resolve().parent.parent)
    if bundle_root not in sys.path:
        sys.path.insert(0, bundle_root)
    from workflows.utils import osm_subsets

    return osm_subsets


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData OSM Subsets Continents & Countries",
    dag_id="openplanetdata_osm_subsets_continents_countries",
    default_args={
        "execution_timeout": timedelta(hours=6),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_osm",
        "priority_weight": 1,
        "queue": "cortex",
        "retries": 0,
        "weight_rule": "absolute",
    },
    description="Continent and country OSM extracts in PBF, GeoParquet, GOL and GOB",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=1,
    schedule=[GOL_V2_ASSET, GEOPARQUET_SHARED_ASSET],
    tags=["continents", "countries", "openplanetdata", "osm", "subsets"],
) as dag:

    @task(task_display_name="Snapshot Planet Inputs")
    def snapshot_inputs() -> None:
        """Hardlink the shared planet GOL and GeoParquet for a stable snapshot.

        The shared files are replaced atomically (rename) by the planet DAGs,
        so hardlinks keep this run's inputs consistent even if the next daily
        planet run finishes mid-flight.
        """
        os.makedirs(WORK_DIR, exist_ok=True)
        for source, snapshot in [
            (SHARED_PLANET_OSM_GOL_PATH, SNAPSHOT_GOL),
            (SHARED_PLANET_OSM_PARQUET_PATH, SNAPSHOT_PARQUET),
        ]:
            if os.path.exists(snapshot):
                os.remove(snapshot)
            os.link(source, snapshot)

    @task.r2index_download(
        task_display_name="Download Continent Boundaries",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_continent_boundaries() -> DownloadItem:
        """Download the planet-wide continents boundary aggregate from R2."""
        return DownloadItem(
            destination=CONTINENTS_AGGREGATE,
            source_filename="planet-latest.continents.geojson",
            source_path="boundaries/continents/planet/geojson",
            source_version="v2",
        )

    @task.r2index_download(
        task_display_name="Download Country Boundaries",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_country_boundaries() -> DownloadItem:
        """Download the planet-wide countries boundary aggregate from R2."""
        return DownloadItem(
            destination=COUNTRIES_AGGREGATE,
            source_filename="planet-latest.countries.geojson",
            source_path="boundaries/countries/planet/geojson",
            source_version="v2",
        )

    @task(task_display_name="Install DuckDB")
    def install_duckdb() -> None:
        """Download the DuckDB CLI once per run into the work directory."""
        subsets = _utils()
        script = "set -euo pipefail\n" + subsets.INSTALL_DUCKDB_TEMPLATE.format(work_dir=WORK_DIR)
        subsets.run_in_container(script)

    @task(task_display_name="Prepare Boundaries")
    def prepare_boundaries() -> list[dict]:
        """Split, buffer and simplify boundaries; return processing batches."""
        from concurrent.futures import ThreadPoolExecutor

        from airflow.exceptions import AirflowException

        subsets = _utils()

        continent_codes = subsets.split_boundary_aggregate(CONTINENTS_AGGREGATE, "slug", BOUNDARIES_DIR)
        country_codes = subsets.split_boundary_aggregate(COUNTRIES_AGGREGATE, "code", BOUNDARIES_DIR)
        print(f"Found {len(continent_codes)} continents and {len(country_codes)} countries")

        with ThreadPoolExecutor(max_workers=4) as executor:
            failures = {
                code for code in executor.map(
                    lambda code: subsets.prepare_boundary(code, BOUNDARIES_DIR),
                    continent_codes + country_codes,
                )
                if code is not None
            }
        if failures:
            # One malformed boundary must not sink the other ~250 subsets;
            # report_failures surfaces the dropped codes at the end of the run.
            print(f"Boundary preparation failed for {len(failures)} subset(s): {sorted(failures)}")
            continent_codes = [c for c in continent_codes if c not in failures]
            country_codes = [c for c in country_codes if c not in failures]
        if not continent_codes and not country_codes:
            raise AirflowException("No boundary could be prepared")

        continent_names = {c["slug"]: c["name"] for c in CONTINENTS}
        country_names = {code: entry["name"] for code, entry in COUNTRIES.items()}

        batches = []
        for i in range(0, len(continent_codes), CONTINENT_BATCH_SIZE):
            codes = continent_codes[i:i + CONTINENT_BATCH_SIZE]
            batches.append({
                "level": "continents",
                "codes": codes,
                "names": {c: continent_names.get(c, c) for c in codes},
            })
        for i in range(0, len(country_codes), COUNTRY_BATCH_SIZE):
            codes = country_codes[i:i + COUNTRY_BATCH_SIZE]
            batches.append({
                "level": "countries",
                "codes": codes,
                "names": {c: country_names.get(c, c) for c in codes},
            })
        return batches

    @task(task_display_name="Process Batch", retries=1)
    def process_batch(batch: dict) -> None:
        """Build PBF/GOL/GOB, extract GeoParquet and upload for one batch."""
        subsets = _utils()
        subsets.process_subset_batch(
            codes=batch["codes"],
            names=batch["names"],
            level=batch["level"],
            level_dir=f"{WORK_DIR}/{batch['level']}",
            boundaries_dir=BOUNDARIES_DIR,
            snapshot_gol=SNAPSHOT_GOL,
            snapshot_parquet=SNAPSHOT_PARQUET,
            work_dir=WORK_DIR,
            r2index_conn_id=R2INDEX_CONNECTION_ID,
            build_workers=BUILD_WORKERS,
        )

    @task(task_display_name="Report Failures", trigger_rule="all_done")
    def report_failures() -> None:
        """Report subsets that never reached a successful upload."""
        missing = []
        for level in ("continents", "countries"):
            level_dir = f"{WORK_DIR}/{level}"
            if not os.path.isdir(level_dir):
                continue
            for entry in sorted(os.listdir(level_dir)):
                path = f"{level_dir}/{entry}"
                if os.path.isdir(path):
                    missing.append(f"{level}/{entry}")
        if missing:
            print(f"{len(missing)} subset(s) not uploaded:")
            for item in missing:
                print(f"  {item}")
        else:
            print("All subsets uploaded successfully.")

    @task(task_id="osm_subsets_continents_countries_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="osm_subsets_continents_countries_cleanup", task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory (snapshots, boundaries, leftovers)."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    snapshot = snapshot_inputs()
    continent_boundaries = download_continent_boundaries()
    country_boundaries = download_country_boundaries()
    batches = prepare_boundaries()

    duckdb_install = install_duckdb()
    snapshot >> [continent_boundaries, country_boundaries]
    [continent_boundaries, country_boundaries] >> duckdb_install >> batches

    process_groups = process_batch.expand(batch=batches)

    report = report_failures()
    process_groups >> report

    done_result = done()
    process_groups >> done_result
    [report, done_result] >> cleanup()
