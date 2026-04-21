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
from airflow.sdk import DAG, Asset, Param, task
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
    dag_id="openplanetdata_osm_geoparquet",
    default_args={
        "execution_timeout": timedelta(hours=6),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "pool": "openplanetdata_osm",
        "queue": "cortex",
        "retries": 0,
        "weight_rule": "elaunira.airflow.priority.OldestFirstPriorityStrategy",
    },
    description="Build planet GeoParquet from OSM PBF using ohsome-planet and DuckDB",
    doc_md=__doc__,
    max_active_runs=1,
    params={
        "multipolygon_member_limit": Param(
            default="-1",
            type="string",
            description="--multipolygon-member-limit for ohsome-planet (-1 for unlimited, 0 to disable, default 500 in ohsome-planet)",
        ),
    },
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
            verify_checksum=False,
        )

    build_contributions = DockerOperator(
        task_id="build_contributions",
        task_display_name="Build Ohsome Contributions",
        image="eclipse-temurin:25-jdk",
        command=f"""bash -c '
            set -euo pipefail

            apt-get update -qq && apt-get install -y -qq git > /dev/null 2>&1

            mkdir -p {WORK_DIR}

            # Clone and build ohsome-planet from main branch
            # (--multipolygon-member-limit support is not yet in a release)
            OHSOME_SRC="{WORK_DIR}/ohsome-planet"
            rm -rf "$OHSOME_SRC"
            git clone --depth 1 --recurse-submodules \
                https://github.com/GIScience/ohsome-planet.git "$OHSOME_SRC"
            cd "$OHSOME_SRC"
            ./mvnw -q clean package -DskipTests
            JAR_PATH="$OHSOME_SRC/ohsome-planet-cli/target/ohsome-planet.jar"

            rm -rf {OHSOME_DIR}
            echo "Building ohsome contributions..."
            time java -Xms84g -Xmx84g -XX:+UseCompactObjectHeaders -jar "$JAR_PATH" \
                contributions --pbf {SHARED_PLANET_OSM_PBF_PATH} --data {OHSOME_DIR} \
                --multipolygon-member-limit {{{{ params.multipolygon_member_limit }}}}

            echo "Contributions build complete"
            ls -lh {OHSOME_DIR}/contributions/
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    INSTALL_DUCKDB = f"""
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)  DUCKDB_ARCH="linux-amd64" ;;
    aarch64) DUCKDB_ARCH="linux-arm64" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

DUCKDB_TAG=""
for i in 1 2 3; do
    RESPONSE=$(curl -sf https://api.github.com/repos/duckdb/duckdb/releases/latest || true)
    DUCKDB_TAG=$(echo "$RESPONSE" | jq -r ".tag_name // empty" 2>/dev/null || true)
    if [ -n "$DUCKDB_TAG" ]; then break; fi
    echo "Attempt $i failed to fetch DuckDB release tag, retrying..."
    sleep 2
done

if [ -z "$DUCKDB_TAG" ]; then
    echo "Failed to fetch DuckDB release tag after 3 attempts"
    exit 1
fi

echo "Installing DuckDB $DUCKDB_TAG ($DUCKDB_ARCH)"
wget -q "https://github.com/duckdb/duckdb/releases/download/${{DUCKDB_TAG}}/duckdb_cli-${{DUCKDB_ARCH}}.zip" \
    -O /tmp/duckdb.zip
unzip -o /tmp/duckdb.zip -d /tmp && chmod +x /tmp/duckdb
"""

    validate_contributions = DockerOperator(
        task_id="validate_contributions",
        task_display_name="Validate Contributions",
        image=OPENPLANETDATA_IMAGE,
        command=["bash", "-c", f"""set -euo pipefail

{INSTALL_DUCKDB}

# Validate parquet files (full scan to detect ZSTD corruption)
echo "Validating parquet files..."
if /tmp/duckdb -c "SELECT COUNT(*) FROM '{OHSOME_DIR}/contributions/*.parquet'" 2>&1; then
    echo "All parquet files validated successfully"
else
    echo "Parquet validation failed, removing corrupted output"
    rm -rf {OHSOME_DIR}
    exit 1
fi
"""],
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    build_geoparquet = DockerOperator(
        task_id="build_geoparquet",
        task_display_name="Build GeoParquet with DuckDB",
        execution_timeout=None,
        image=OPENPLANETDATA_IMAGE,
        command=["bash", "-c", f"""set -euo pipefail

{INSTALL_DUCKDB}

# Process with DuckDB spatial extension
DUCKDB_TEMP_DIR="{WORK_DIR}/.duckdb-temp"
mkdir -p "$DUCKDB_TEMP_DIR"

cat /proc/meminfo | head -3 || true
echo "Input parquet files:"
ls -lh {OHSOME_DIR}/contributions/*.parquet | head -5 || true
echo "..."
ls {OHSOME_DIR}/contributions/*.parquet | wc -l || true
echo " parquet file(s) total"
echo "Starting DuckDB processing..."

set +e
/tmp/duckdb -c "
    INSTALL 'spatial'; LOAD 'spatial';
    SET temp_directory='$DUCKDB_TEMP_DIR';
    SET memory_limit='65GB';
    SET preserve_insertion_order=false;

    COPY (
        SELECT
            osm_type::ENUM ('node', 'way', 'relation') AS osm_type,
            osm_id,
            tags,
            bbox,
            CASE
                WHEN osm_type = 'relation'
                     AND ST_NPoints(geometry) = 5
                     AND ST_Equals(geometry, ST_Envelope(geometry))
                     AND members IS NOT NULL
                THEN ST_Collect(list_transform(
                    list_filter(members, lambda m: m.geometry IS NOT NULL),
                    lambda m: ST_GeomFromWKB(m.geometry)
                ))
                ELSE geometry
            END AS geometry
        FROM '{OHSOME_DIR}/contributions/*.parquet'
        ORDER BY bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax
    ) TO '{PARQUET_PATH}' (
        FORMAT PARQUET,
        CODEC 'zstd',
        COMPRESSION_LEVEL 6,
        PARQUET_VERSION v2
    );
"
DUCKDB_EXIT=$?
set -e

if [ $DUCKDB_EXIT -ne 0 ]; then
    echo "DuckDB failed with exit code $DUCKDB_EXIT"
    if [ $DUCKDB_EXIT -eq 137 ]; then
        echo "Exit code 137 indicates the process was killed (likely OOM)"
    fi
    cat /proc/meminfo | head -3 || true
    exit $DUCKDB_EXIT
fi

rm -rf "$DUCKDB_TEMP_DIR"
echo "GeoParquet processing complete"
ls -lh {PARQUET_PATH}
"""],
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
            category="openstreetmap",
            name="Planet",
            subcategory="planet",
            destination_filename="planet-latest.osm.parquet",
            destination_path="osm/planet/geoparquet",
            destination_version="v1",
            entity="planet-geoparquet",
            extension="parquet",
            media_type="application/vnd.apache.parquet",
            source=PARQUET_PATH,
            tags=["aggregate", "geoparquet", "openstreetmap"],
        )]

    @task(task_id="osm_geoparquet_done", task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_id="osm_geoparquet_cleanup", task_display_name="Cleanup")
    def cleanup() -> None:
        """Clean up working directory after successful run."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_result = download_planet_pbf()
    download_result >> build_contributions >> validate_contributions >> build_geoparquet

    upload_result = upload_geoparquet()
    build_geoparquet >> upload_result
    upload_result >> done()
    upload_result >> cleanup()
