"""
Planet GeoDesk v2 DAG - Build GOL and GOB indexes from OSM planet PBF.

Schedule: Triggered by openplanetdata-osm-planet-pbf Asset
Produces Assets: openplanetdata-osm-planet-gol-v2, openplanetdata-osm-planet-gob-v2
"""

import os
import shutil
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, task
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import DownloadItem, UploadItem
from elaunira.r2index.storage import R2TransferConfig
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    OPENPLANETDATA_IMAGE,
    OPENPLANETDATA_SHARED_DIR,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_GOL_PATH,
    SHARED_PLANET_OSM_PBF_PATH,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/geodesk/v2"
GOL_PATH = f"{WORK_DIR}/planet-latest.osm.gol"
GOB_PATH = f"{WORK_DIR}/planet-latest.osm.gob"

PBF_ASSET = Asset(
    name="openplanetdata-osm-planet-pbf",
    uri=f"s3://{R2_BUCKET}/osm/planet/pbf/v1/planet-latest.osm.pbf",
)
GOL_V2_ASSET = Asset(
    name="openplanetdata-osm-planet-gol-v2",
    uri=f"s3://{R2_BUCKET}/osm/planet/gol/v2/planet-latest.osm.gol",
)
GOB_V2_ASSET = Asset(
    name="openplanetdata-osm-planet-gob-v2",
    uri=f"s3://{R2_BUCKET}/osm/planet/gob/v2/planet-latest.osm.gob",
)

with DAG(
    dag_display_name="OpenPlanetData OSM Planet GeoDesk v2",
    dag_id="openplanetdata_osm_geodesk_v2",
    default_args={
        "execution_timeout": timedelta(hours=4),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_osm",
        "queue": "cortex",
        "retries": 0,
        "weight_rule": "elaunira.airflow.priority.OldestFirstPriorityStrategy",
    },
    description="Build GeoDesk GOL and GOB indexes from OSM planet PBF",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=PBF_ASSET,
    tags=["geodesk", "gob", "gol", "openplanetdata", "osm", "planet"],
) as dag:

    @task.r2index_download(
        task_display_name="Download Planet PBF",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        transfer_config=R2TransferConfig(max_concurrency=64, multipart_chunksize=32 * 1024 * 1024),
    )
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=SHARED_PLANET_OSM_PBF_PATH,
            overwrite=False,
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="v1",
            verify_checksum=False,
        )

    build_gol = DockerOperator(
        task_id="build_gol",
        task_display_name="Build GOL v2",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            mkdir -p {WORK_DIR}/.tmp &&
            rm -rf {WORK_DIR}/*.osm-work &&
            echo "Starting GOL build at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            time gol build --updatable --yes {GOL_PATH} {SHARED_PLANET_OSM_PBF_PATH} &&
            echo "Build finished at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            ls -lh {GOL_PATH}
        '""",
        environment={"TMPDIR": f"{WORK_DIR}/.tmp"},
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    build_gob = DockerOperator(
        task_id="build_gob",
        task_display_name="Build GOB from GOL",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            echo "Starting GOB build at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            time gol save {GOL_PATH} {GOB_PATH} &&
            echo "Build finished at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            ls -lh {GOB_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload GOL to R2",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_gol() -> list[UploadItem]:
        """Upload GOL v2 to R2."""
        return [UploadItem(
            category="openstreetmap",
            name="Planet",
            subcategory="planet",
            destination_filename="planet-latest.osm.gol",
            destination_path="osm/planet/gol",
            destination_version="v2",
            entity="planet-gol",
            extension="gol",
            media_type="application/octet-stream",
            source=GOL_PATH,
            tags=["geodesk", "gol", "openstreetmap"],
        )]

    @task.r2index_upload(
        task_display_name="Upload GOB to R2",
        bucket=R2_BUCKET,
        outlets=[GOB_V2_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_gob() -> list[UploadItem]:
        """Upload GOB to R2."""
        return [UploadItem(
            category="openstreetmap",
            name="Planet",
            subcategory="planet",
            destination_filename="planet-latest.osm.gob",
            destination_path="osm/planet/gob",
            destination_version="v1",
            entity="planet-gob",
            extension="gob",
            media_type="application/octet-stream",
            source=GOB_PATH,
            tags=["geodesk", "gob", "gol", "openstreetmap"],
        )]

    @task(task_display_name="Copy GOL to Shared Directory", outlets=[GOL_V2_ASSET])
    def copy_to_shared() -> None:
        """Copy GOL to shared directory atomically for use by other DAGs."""
        os.makedirs(OPENPLANETDATA_SHARED_DIR, exist_ok=True)
        tmp_path = f"{SHARED_PLANET_OSM_GOL_PATH}.tmp"
        shutil.copy2(GOL_PATH, tmp_path)
        os.rename(tmp_path, SHARED_PLANET_OSM_GOL_PATH)

    @task(task_id="osm_geodesk_v2_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="osm_geodesk_v2_cleanup", task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_result = download_planet_pbf()
    download_result >> build_gol

    gol_upload = upload_gol()
    gob_upload = upload_gob()

    build_gol >> gol_upload
    build_gol >> build_gob >> gob_upload

    copy_result = copy_to_shared()
    build_gol >> copy_result

    done_result = done()
    cleanup_result = cleanup()

    [gol_upload, gob_upload, copy_result] >> done_result
    [gol_upload, gob_upload, copy_result] >> cleanup_result
