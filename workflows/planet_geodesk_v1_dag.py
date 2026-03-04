"""
Planet GeoDesk v1 DAG - Build GOL v1 index from OSM planet PBF (deprecated).

Schedule: Triggered by openplanetdata-osm-planet-pbf Asset
Produces Asset: openplanetdata-osm-planet-gol-v1

Deprecated: Users should switch to GeoDesk v2; support for v1 will be
removed after 30/09/2026.
"""

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
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_PBF_PATH,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/geodesk/v1"
GOL_PATH = f"{WORK_DIR}/planet-latest.osm.gol"

PBF_ASSET = Asset(
    name="openplanetdata-osm-planet-pbf",
    uri=f"s3://{R2_BUCKET}/osm/planet/pbf/v1/planet-latest.osm.pbf",
)
GOL_V1_ASSET = Asset(
    name="openplanetdata-osm-planet-gol-v1",
    uri=f"s3://{R2_BUCKET}/osm/planet/gol/v1/planet-latest.osm.gol",
)

with DAG(
    dag_display_name="OpenPlanetData OSM Planet GeoDesk v1 (deprecated)",
    dag_id="openplanetdata_osm_planet_geodesk_v1",
    default_args={
        "execution_timeout": timedelta(hours=4),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
        "retries": 0,
    },
    description="Build GeoDesk GOL v1 index from OSM planet PBF (deprecated)",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=PBF_ASSET,
    tags=["deprecated", "geodesk", "gol", "openplanetdata", "osm", "planet"],
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
        )

    build_gol = DockerOperator(
        task_id="build_gol",
        task_display_name="Build GOL v1",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            mkdir -p {WORK_DIR}/.tmp &&
            echo "Installing GOL v1..." &&
            GOL_URL=$(curl -sSL https://api.github.com/repos/clarisma/gol-tool/releases/latest \
                | grep -o "https://[^\\"]*gol-tool-[^\\"]*\\.zip" | head -n1) &&
            echo "Downloading GOL v1 from $GOL_URL" &&
            curl -sSL "$GOL_URL" -o /tmp/gol-v1.zip &&
            unzip -q -o /tmp/gol-v1.zip -d /tmp/gol-v1 &&
            GOL_BIN=$(find /tmp/gol-v1 -name gol -type f | head -n1) &&
            chmod +x "$GOL_BIN" &&
            echo "Starting GOL v1 build at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            echo "JVM options: $JAVA_TOOL_OPTIONS" &&
            time "$GOL_BIN" build --tag-duplicate-nodes=yes {GOL_PATH} {SHARED_PLANET_OSM_PBF_PATH} &&
            echo "Build finished at $(date -u +%Y-%m-%d_%H:%M:%S)" &&
            ls -lh {GOL_PATH}
        '""",
        environment={
            "JAVA_TOOL_OPTIONS": "-Xms96g -Xmx96g",
            "TMPDIR": f"{WORK_DIR}/.tmp",
        },
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload GOL to R2",
        bucket=R2_BUCKET,
        outlets=[GOL_V1_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_gol() -> list[UploadItem]:
        """Upload GOL v1 to R2 (deprecated)."""
        return [UploadItem(
            category="openstreetmap",
            destination_filename="planet-latest.osm.gol",
            destination_path="osm/planet/gol",
            destination_version="v1",
            entity="planet",
            extension="gol",
            deprecated=True,
            deprecation_reason=(
                "Users should switch to GeoDesk v2 and associated files; "
                "support for v1 will be removed after 30/09/2026."
            ),
            media_type="application/octet-stream",
            source=GOL_PATH,
            tags=["deprecated", "geodesk", "gol", "openstreetmap", "public"],
        )]

    @task(task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_result = download_planet_pbf()
    download_result >> build_gol

    gol_upload = upload_gol()
    build_gol >> gol_upload
    gol_upload >> done()
    gol_upload >> cleanup()
