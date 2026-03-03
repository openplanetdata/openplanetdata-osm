"""
Planet GeoParquet DAG - Build planet-scale GeoParquet from OSM PBF.

Uses ohsome-planet for contribution extraction and DuckDB for spatial
processing, validation, and output compression.

Schedule: Triggered by openplanetdata-osm-planet-pbf Asset
Produces Asset: openplanetdata-osm-planet-geoparquet
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

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/osm/geoparquet"
OHSOME_DIR = f"{WORK_DIR}/ohsome-output"
PARQUET_PATH = f"{WORK_DIR}/planet-latest.osm.parquet"

PBF_ASSET = Asset(
    name="openplanetdata-osm-planet-pbf",
    uri=f"s3://{R2_BUCKET}/osm/planet/pbf/v1/planet-latest.osm.pbf",
)
GEOPARQUET_ASSET = Asset(
    name="openplanetdata-osm-planet-geoparquet",
    uri=f"s3://{R2_BUCKET}/osm/planet/geoparquet/v1/planet-latest.osm.parquet",
)

with DAG(
    dag_display_name="OpenPlanetData OSM Planet GeoParquet",
    dag_id="openplanetdata_osm_planet_geoparquet",
    default_args={
        "execution_timeout": timedelta(hours=6),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
        "retries": 0,
    },
    description="Build planet GeoParquet from OSM PBF using ohsome-planet and DuckDB",
    doc_md=__doc__,
    max_active_runs=1,
    schedule=PBF_ASSET,
    tags=["geoparquet", "openplanetdata", "osm", "planet"],
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

    build_contributions = DockerOperator(
        task_id="build_contributions",
        task_display_name="Build Ohsome Contributions",
        image="eclipse-temurin:25-jdk",
        command=f"""bash -c '
            set -euo pipefail

            apt-get update -qq && apt-get install -y -qq curl git jq > /dev/null 2>&1

            mkdir -p {WORK_DIR}

            # Get latest ohsome-planet release tag
            OHSOME_TAG=""
            for i in 1 2 3; do
                RESPONSE=$(curl -sf https://api.github.com/repos/GIScience/ohsome-planet/releases/latest || true)
                OHSOME_TAG=$(echo "$RESPONSE" | jq -r ".tag_name // empty" 2>/dev/null || true)
                if [ -n "$OHSOME_TAG" ]; then
                    echo "Using ohsome-planet release: $OHSOME_TAG"
                    break
                fi
                echo "Attempt $i failed to fetch ohsome-planet release tag, retrying..."
                sleep 2
            done

            if [ -z "$OHSOME_TAG" ]; then
                echo "Failed to fetch ohsome-planet release tag after 3 attempts"
                exit 1
            fi

            # Clone and build ohsome-planet
            OHSOME_SRC="{WORK_DIR}/ohsome-planet"
            if [ ! -d "$OHSOME_SRC" ]; then
                git clone --branch "$OHSOME_TAG" --depth 1 --recurse-submodules \
                    https://github.com/GIScience/ohsome-planet.git "$OHSOME_SRC"
            fi
            cd "$OHSOME_SRC"
            ./mvnw -q clean package -DskipTests
            JAR_PATH=$(find "$OHSOME_SRC" -name "ohsome-planet*.jar" -path "*/target/*" \
                -not -name "*sources*" -not -name "*javadoc*" | head -1)

            # Check if contributions already exist (caching for retries)
            if [ -d "{OHSOME_DIR}/contributions/latest" ]; then
                echo "Found existing ohsome output, skipping build"
            else
                rm -rf {OHSOME_DIR}
                echo "Building ohsome contributions..."
                time java -Xms96g -Xmx96g -jar "$JAR_PATH" \
                    contributions --pbf {SHARED_PLANET_OSM_PBF_PATH} --data {OHSOME_DIR}
            fi

            echo "Contributions build complete"
            ls -lh {OHSOME_DIR}/contributions/latest/
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    validate_and_process = DockerOperator(
        task_id="validate_and_process",
        task_display_name="Validate and Process with DuckDB",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            set -euo pipefail

            # Install DuckDB
            ARCH=$(uname -m)
            case "$ARCH" in
                x86_64)  DUCKDB_ARCH="linux-amd64" ;;
                aarch64) DUCKDB_ARCH="linux-arm64" ;;
                *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
            esac

            DUCKDB_TAG=""
            for i in 1 2 3; do
                DUCKDB_TAG=$(curl -sf https://api.github.com/repos/duckdb/duckdb/releases/latest | \
                    grep -o "\"tag_name\": \"[^\"]*\"" | cut -d\" -f4 || true)
                if [ -n "$DUCKDB_TAG" ]; then break; fi
                sleep 2
            done

            if [ -z "$DUCKDB_TAG" ]; then
                echo "Failed to fetch DuckDB release tag"
                exit 1
            fi

            echo "Installing DuckDB $DUCKDB_TAG ($DUCKDB_ARCH)"
            wget -q "https://github.com/duckdb/duckdb/releases/download/${{DUCKDB_TAG}}/duckdb_cli-${{DUCKDB_ARCH}}.zip" \
                -O /tmp/duckdb.zip
            unzip -o /tmp/duckdb.zip -d /tmp && chmod +x /tmp/duckdb

            # Validate parquet files (full scan to detect ZSTD corruption)
            echo "Validating parquet files..."
            VALIDATION_FAILED=0
            for parquet_file in {OHSOME_DIR}/contributions/latest/*.parquet; do
                if [ -f "$parquet_file" ]; then
                    echo "Validating: $parquet_file"
                    if ! /tmp/duckdb -c "SELECT MAX(osm_id) FROM '\''$parquet_file'\''" > /dev/null 2>&1; then
                        echo "Corrupted parquet file: $parquet_file"
                        VALIDATION_FAILED=1
                    fi
                fi
            done

            if [ $VALIDATION_FAILED -eq 1 ]; then
                echo "Parquet validation failed, removing corrupted output"
                rm -rf {OHSOME_DIR}
                exit 1
            fi
            echo "All parquet files validated successfully"

            # Process with DuckDB spatial extension
            DUCKDB_TEMP_DIR="{WORK_DIR}/.duckdb-temp"
            mkdir -p "$DUCKDB_TEMP_DIR"

            /tmp/duckdb -c "
                INSTALL '\''spatial'\''; LOAD '\''spatial'\'';
                SET temp_directory='\''$DUCKDB_TEMP_DIR'\'';
                SET preserve_insertion_order=false;

                COPY (
                    SELECT
                        osm_type::ENUM ('\''node'\'', '\''way'\'', '\''relation'\'') AS osm_type,
                        osm_id,
                        tags,
                        bbox,
                        geometry
                    FROM '\''{OHSOME_DIR}/contributions/latest/*.parquet'\''
                    ORDER BY bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax
                ) TO '\''{PARQUET_PATH}'\'' (
                    FORMAT PARQUET,
                    CODEC '\''zstd'\'',
                    COMPRESSION_LEVEL 13,
                    PARQUET_VERSION v2
                );
            "

            rm -rf "$DUCKDB_TEMP_DIR"
            echo "GeoParquet processing complete"
            ls -lh {PARQUET_PATH}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(
        task_display_name="Upload GeoParquet to R2",
        bucket=R2_BUCKET,
        outlets=[GEOPARQUET_ASSET],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geoparquet() -> list[UploadItem]:
        """Upload planet GeoParquet to R2."""
        return [UploadItem(
            category="geoparquet",
            destination_filename="planet-latest.osm.parquet",
            destination_path="osm/planet/geoparquet",
            destination_version="v1",
            entity="planet",
            extension="parquet",
            media_type="application/vnd.apache.parquet",
            source=PARQUET_PATH,
            tags=["geoparquet", "openstreetmap", "public"],
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
    download_result >> build_contributions >> validate_and_process

    upload_result = upload_geoparquet()
    validate_and_process >> upload_result
    upload_result >> done()
    upload_result >> cleanup()
