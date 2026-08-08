"""
OSM Subsets Regions DAG - ISO3166-2 region extracts in PBF, GeoParquet, GOL and GOB.

Schedule: Weekly Sunday 12:00 UTC - late enough that the Sunday planet
pipeline (started 01:00 UTC) is normally finished, so region batches do not
interleave with planet tasks. (Regions are ~3,000+ subsets; a full sweep does
not fit the daily budget on the single worker - promote to daily only if
measured runtimes allow.)

Same pipeline and scheduling policy as the continents & countries subsets DAG
(see workflows/utils/osm_subsets.py and subsets_continents_countries_dag.py):
strictly low
priority in the shared openplanetdata_osm pool, one task at a time, so queued
planet tasks always win the next free slot (a running batch is never
preempted, but batches are small so any wait is bounded by one batch). Region
codes come from the weekly boundaries aggregate; regions whose boundary
matches nothing are skipped and reported rather than failing the run.
"""

import os
import shutil
import sys
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

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/subsets/regions"
BOUNDARIES_DIR = f"{WORK_DIR}/boundaries"
SNAPSHOT_GOL = f"{WORK_DIR}/planet-latest.osm.gol"
SNAPSHOT_PARQUET = f"{WORK_DIR}/planet-latest.osm.parquet"

REGIONS_AGGREGATE = f"{WORK_DIR}/planet-latest.regions.geojson"

REGION_BATCH_SIZE = 32
BUILD_WORKERS = 2


def _utils():
    """Import workflows.utils.osm_subsets at task runtime (bundle-relative)."""
    bundle_root = str(Path(__file__).resolve().parent.parent)
    if bundle_root not in sys.path:
        sys.path.insert(0, bundle_root)
    from workflows.utils import osm_subsets

    return osm_subsets


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData OSM Subsets Regions",
    dag_id="openplanetdata_osm_subsets_regions",
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
    description="ISO3166-2 region OSM extracts in PBF, GeoParquet, GOL and GOB",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=1,
    schedule="0 12 * * 0",
    tags=["openplanetdata", "osm", "regions", "subsets"],
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
                raise AirflowException(f"Missing shared planet input: {source}")
            if os.path.exists(snapshot):
                os.remove(snapshot)
            os.link(source, snapshot)

    @task.r2index_download(
        task_display_name="Download Region Boundaries",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_region_boundaries() -> DownloadItem:
        """Download the planet-wide regions boundary aggregate from R2."""
        return DownloadItem(
            destination=REGIONS_AGGREGATE,
            source_filename="planet-latest.regions.geojson",
            source_path="boundaries/regions/planet/geojson",
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
        """Split, buffer and simplify region boundaries; return processing batches."""
        from concurrent.futures import ThreadPoolExecutor

        from airflow.exceptions import AirflowException

        subsets = _utils()

        region_codes = subsets.split_boundary_aggregate(REGIONS_AGGREGATE, "code", BOUNDARIES_DIR)
        print(f"Found {len(region_codes)} regions")

        with ThreadPoolExecutor(max_workers=4) as executor:
            failures = [
                code for code in executor.map(
                    lambda code: subsets.prepare_boundary(code, BOUNDARIES_DIR),
                    region_codes,
                )
                if code is not None
            ]
        if failures:
            # A handful of broken region boundaries must not sink ~3,000 others.
            print(f"Boundary preparation failed for {len(failures)} region(s): {sorted(failures)}")
            region_codes = [c for c in region_codes if c not in set(failures)]
        if not region_codes:
            raise AirflowException("No region boundary could be prepared")

        return [
            {
                "level": "regions",
                "codes": region_codes[i:i + REGION_BATCH_SIZE],
                "names": {},
            }
            for i in range(0, len(region_codes), REGION_BATCH_SIZE)
        ]

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
        """Report regions that never reached a successful upload."""
        level_dir = f"{WORK_DIR}/regions"
        missing = []
        if os.path.isdir(level_dir):
            missing = [
                entry for entry in sorted(os.listdir(level_dir))
                if os.path.isdir(f"{level_dir}/{entry}")
            ]
        if missing:
            print(f"{len(missing)} region(s) not uploaded:")
            for code in missing:
                print(f"  {code}")
        else:
            print("All regions uploaded successfully.")

    @task(task_id="osm_subsets_regions_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="osm_subsets_regions_cleanup", task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory (snapshots, boundaries, leftovers)."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    snapshot = snapshot_inputs()
    boundaries = download_region_boundaries()
    duckdb_install = install_duckdb()
    batches = prepare_boundaries()

    snapshot >> boundaries >> duckdb_install >> batches

    process_groups = process_batch.expand(batch=batches)

    report = report_failures()
    process_groups >> report

    done_result = done()
    process_groups >> done_result
    [report, done_result] >> cleanup()
