"""
Planet PBF DAG - Daily OSM planet PBF download and replication update.

Schedule: Daily at 01:00 UTC
Produces Asset: openplanetdata-osm-planet-pbf (triggers downstream DAGs)
"""

import shutil
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, task
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import UploadItem
from elaunira.r2index.storage import R2TransferConfig
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    OPENPLANETDATA_IMAGE,
    OPENPLANETDATA_SHARED_DIR,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_PBF_PATH,
)

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/pbf"
PBF_PATH = f"{WORK_DIR}/planet-latest.osm.pbf"

PBF_ASSET = Asset(
    name="openplanetdata-osm-planet-pbf",
    uri=f"s3://{R2_BUCKET}/osm/planet/pbf/v1/planet-latest.osm.pbf",
)

with DAG(
    dag_display_name="OpenPlanetData OSM Planet PBF",
    dag_id="openplanetdata_osm_pbf",
    default_args={
        "execution_timeout": timedelta(hours=4),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_osm",
        "queue": "cortex",
        "retries": 0,
        "weight_rule": "elaunira.airflow.priority.OldestFirstPriorityStrategy",
    },
    description="Daily OSM planet PBF download via torrent and replication update",
    doc_md=__doc__,
    max_active_runs=1,
    schedule="0 1 * * *",
    tags=["openplanetdata", "osm", "pbf", "planet"],
) as dag:

    download_planet = DockerOperator(
        task_id="download_planet_pbf",
        task_display_name="Download Planet PBF via Torrent",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            mkdir -p {WORK_DIR} &&
            cd {WORK_DIR} &&
            aria2c \
                --allow-overwrite=true \
                --bt-max-peers=500 \
                --bt-request-peer-speed-limit=0 \
                --bt-save-metadata=false \
                --continue=true \
                --disk-cache=0 \
                --enable-dht=true \
                --enable-peer-exchange=true \
                --file-allocation=falloc \
                --max-connection-per-server=16 \
                --max-download-limit=0 \
                --max-overall-download-limit=0 \
                --min-split-size=10M \
                --seed-time=0 \
                --split=128 \
                --summary-interval=10 \
                https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf.torrent \
                --index-out=1=planet-latest.osm.pbf &&
            ls -lh {PBF_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    update_planet = DockerOperator(
        task_id="update_planet_pbf",
        task_display_name="Update PBF to Latest Replication State",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            cd {WORK_DIR} &&
            set +e
            status=1
            iteration=0

            while [ "$status" -eq 1 ]; do
                iteration=$((iteration + 1))
                echo "=== Update iteration $iteration ==="
                pyosmium-up-to-date \
                    -vv \
                    --server https://planet.openstreetmap.org/replication/hour \
                    --size 2048 \
                    planet-latest.osm.pbf
                status=$?
                echo "Exit status: $status"
                if [ "$status" -gt 1 ]; then
                    echo "Error: pyosmium-up-to-date failed with status $status"
                    exit "$status"
                elif [ "$status" -eq 1 ]; then
                    echo "More updates available, continuing..."
                else
                    echo "Update complete!"
                fi
            done

            echo ""
            echo "=== Grace period: Running additional updates to ensure fully caught up ==="
            for grace in 1 2 3; do
                echo "--- Grace update attempt $grace ---"
                pyosmium-up-to-date \
                    -vv \
                    --server https://planet.openstreetmap.org/replication/hour \
                    --size 2048 \
                    planet-latest.osm.pbf
                grace_status=$?
                echo "Grace attempt $grace exit status: $grace_status"
                if [ "$grace_status" -gt 1 ]; then
                    echo "Error during grace period update"
                    exit "$grace_status"
                elif [ "$grace_status" -eq 0 ]; then
                    echo "No additional updates needed"
                    break
                fi
            done

            echo ""
            echo "=== Final verification ==="
            osmium fileinfo -e planet-latest.osm.pbf || true
            rm -f *.torrent
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload PBF to R2",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        transfer_config=R2TransferConfig(max_concurrency=16, multipart_chunksize=256 * 1024 * 1024),
    )
    def upload_pbf() -> list[UploadItem]:
        """Upload planet PBF to R2."""
        return [UploadItem(
            category="openstreetmap",
            name="Planet",
            subcategory="planet",
            destination_filename="planet-latest.osm.pbf",
            destination_path="osm/planet/pbf",
            destination_version="v1",
            entity="planet-pbf",
            extension="pbf",
            media_type="application/x-protobuf",
            source=PBF_PATH,
            tags=["openstreetmap", "pbf"],
        )]

    @task(task_display_name="Copy to Shared Directory", outlets=[PBF_ASSET])
    def copy_to_shared() -> None:
        """Copy planet PBF to shared directory for use by other DAGs."""
        import os

        os.makedirs(OPENPLANETDATA_SHARED_DIR, exist_ok=True)
        shutil.copy2(PBF_PATH, SHARED_PLANET_OSM_PBF_PATH)

    @task(task_id="osm_pbf_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="osm_pbf_cleanup", task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_planet >> update_planet
    upload_result = upload_pbf()
    update_planet >> upload_result
    copy_result = copy_to_shared()
    upload_result >> copy_result
    copy_result >> done()
    copy_result >> cleanup()
