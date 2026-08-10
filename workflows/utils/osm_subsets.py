"""Shared helpers for the OSM subset DAGs (continents, countries, regions).

Every subset is produced with the same four-step pipeline against snapshots of
the daily planet artifacts:

1. PBF        gol query <planet.gol> --area <boundary> -f pbf   (GOL >= 2.3),
              or osmium extract from <planet.osm.pbf> for planet-scale areas
2. GOL v2     gol build from the subset PBF
3. GOB        gol save from the subset GOL
4. GeoParquet DuckDB COPY from the planet GeoParquet (bbox prefilter for
              row-group pruning + ST_Intersects with the boundary)

Boundary polygons come from the openplanetdata-boundaries planet aggregates and
are pre-simplified (0.005 deg) per input geometry, then unioned, buffered
(0.02 deg) and simplified (0.01 deg) once per run: the raw coastline-clipped
boundaries have millions of vertices (europe is a ~620 MB GeoJSON), which
would cripple both gol's and DuckDB's point-in-polygon tests - and buffering
them at full resolution exhausts GEOS memory (a full-res europe buffer got
OOM-killed along with the edge worker). Pre-simplification error (0.005) plus
final simplification error (0.01) stays below the 0.02 buffer, so the result
remains a superset of the land boundary: nearshore objects are retained,
deep-offshore objects are excluded (land-extract semantics).
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
from typing import Any

from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    GDAL_FULL_IMAGE,
    OPENPLANETDATA_IMAGE,
    R2_BUCKET,
)

BOUNDARY_BUFFER_DEG = 0.02
BOUNDARY_SIMPLIFY_DEG = 0.01
BOUNDARY_PRESIMPLIFY_DEG = 0.005

# Hard cap for the boundary-prep GDAL container: a runaway ST_Buffer must die
# alone instead of triggering the host OOM killer (which also takes down the
# Airflow edge worker and loses the task logs).
BOUNDARY_PREP_MEM_LIMIT = "64g"

# Same protection for the gol containers: gol 2.3's PBF exporter buffers the
# whole result set in memory (a europe extract reached ~120 GiB RSS on the
# 124 GiB host and the global OOM killer took out neighboring pods and the
# edge worker three runs in a row).
GOL_MEM_LIMIT = "100g"

# osmium's complete_ways extractor uses compact ID bitsets and two input passes,
# but keep a generous hard cap so regressions cannot take down the edge worker.
OSMIUM_MEM_LIMIT = "32g"
OSMIUM_IMAGE = "docker.io/iboates/osmium:1.19.0"

# A PBF smaller than this holds only a header: the boundary matched nothing.
EMPTY_PBF_THRESHOLD_BYTES = 1024

SUBSET_FORMATS = [
    # (format key, file suffix, R2 subfolder, R2 version, media type, tags)
    ("pbf", "osm.pbf", "pbf", "v1", "application/x-protobuf", ["openstreetmap", "pbf"]),
    ("geoparquet", "osm.parquet", "geoparquet", "v1", "application/vnd.apache.parquet", ["geoparquet", "openstreetmap"]),
    ("gol", "osm.gol", "gol", "v2", "application/octet-stream", ["geodesk", "gol", "openstreetmap"]),
    ("gob", "osm.gob", "gob", "v1", "application/octet-stream", ["geodesk", "gob", "gol", "openstreetmap"]),
]

INSTALL_DUCKDB_TEMPLATE = """
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
    -O {work_dir}/duckdb.zip
unzip -o {work_dir}/duckdb.zip -d {work_dir} && chmod +x {work_dir}/duckdb
rm -f {work_dir}/duckdb.zip
"""


_PULLED_IMAGES: set[str] = set()


def run_in_container(
    cmd: str | list[str],
    image: str = OPENPLANETDATA_IMAGE,
    env: dict | None = None,
    stdout_only: bool = False,
    mem_limit: str | None = None,
    shell: bool = True,
) -> bytes:
    """Run a command in a Docker container with the /data mount.

    Thread-safe (Docker SDK). The image is pulled once per process (mirroring
    DockerOperator's force_pull). The container is started detached and force-
    removed in a finally block, so an exception raised in the calling thread
    (task kill, timeout) kills the container instead of orphaning it. Raises
    docker.errors.ContainerError on non-zero exit. By default cmd runs through
    bash; set shell=False for images that expose their CLI as the entrypoint.
    Returns the stdout logs (plus stderr unless stdout_only).
    """
    import docker
    from docker.errors import ContainerError
    from docker.types import Mount

    from openplanetdata.airflow.operators.gol import DOCKER_USER

    client = docker.from_env()
    if image not in _PULLED_IMAGES:
        client.images.pull(image)
        _PULLED_IMAGES.add(image)

    if shell:
        if not isinstance(cmd, str):
            raise TypeError("shell commands must be strings")
        container_command: str | list[str] = f"bash -c {shlex.quote(cmd)}"
    else:
        container_command = cmd

    container = client.containers.run(
        image=image,
        command=container_command,
        detach=True,
        environment=env or {},
        mem_limit=mem_limit,
        mounts=[Mount(**DOCKER_MOUNT)],
        user=DOCKER_USER,
    )
    try:
        status = container.wait()["StatusCode"]
        if status != 0:
            stderr = container.logs(stdout=False, stderr=True)
            raise ContainerError(container, status, cmd, image, stderr)
        return container.logs(stdout=True, stderr=not stdout_only)
    finally:
        container.remove(force=True)


def _geometry_bbox(geometry: dict) -> tuple[float, float, float, float]:
    """Compute (minx, miny, maxx, maxy) of a GeoJSON geometry."""
    minx = miny = float("inf")
    maxx = maxy = float("-inf")

    def visit(coords: Any) -> None:
        nonlocal minx, miny, maxx, maxy
        if isinstance(coords[0], (int, float)):
            x, y = coords[0], coords[1]
            minx, miny = min(minx, x), min(miny, y)
            maxx, maxy = max(maxx, x), max(maxy, y)
        else:
            for part in coords:
                visit(part)

    visit(geometry["coordinates"])
    return minx, miny, maxx, maxy


def split_boundary_aggregate(aggregate_path: str, code_property: str, boundaries_dir: str) -> list[str]:
    """Split a planet boundary aggregate into per-code raw GeoJSON files.

    Writes {boundaries_dir}/{code}.raw.geojson with layer name "boundary" (used
    by the simplify SQL). Returns the sorted list of codes found.
    """
    os.makedirs(boundaries_dir, exist_ok=True)
    with open(aggregate_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    features_by_code: dict[str, list] = {}
    for feature in data.get("features", []):
        code = (feature.get("properties") or {}).get(code_property)
        if code and feature.get("geometry"):
            features_by_code.setdefault(code, []).append(feature)

    for code, features in features_by_code.items():
        with open(f"{boundaries_dir}/{code}.raw.geojson", "w", encoding="utf-8") as fh:
            json.dump({"type": "FeatureCollection", "name": "boundary", "features": features}, fh)

    return sorted(features_by_code.keys())


def prepare_boundary(code: str, boundaries_dir: str) -> str | None:
    """Buffer + simplify one raw boundary and write its metadata sidecar.

    Produces {code}.prepared.geojson (single unioned feature) and {code}.meta.json
    (bbox + geometry). Thread-safe. Returns the code on failure, None on success.
    """
    raw_path = f"{boundaries_dir}/{code}.raw.geojson"
    prepared_path = f"{boundaries_dir}/{code}.prepared.geojson"
    meta_path = f"{boundaries_dir}/{code}.meta.json"

    if os.path.exists(meta_path):
        return None

    try:
        # Pre-simplify each input geometry BEFORE union/buffer: buffering the
        # full-resolution coastline is what exhausts memory, not the union.
        # Plain ST_Simplify (Douglas-Peucker), not ST_SimplifyPreserveTopology:
        # the topology-preserving variant needs >1h of CPU on the ~25M-vertex
        # europe boundary while DP takes seconds. DP may self-intersect, so
        # ST_Buffer(.., 0) repairs each geometry before the union.
        sql = (
            f"SELECT ST_SimplifyPreserveTopology(ST_Buffer(ST_Union("
            f"ST_Buffer(ST_Simplify(geometry, {BOUNDARY_PRESIMPLIFY_DEG}), 0)), "
            f"{BOUNDARY_BUFFER_DEG}), {BOUNDARY_SIMPLIFY_DEG}) AS geometry FROM boundary"
        )
        args = shlex.join([
            "ogr2ogr", "-f", "GeoJSON", prepared_path, raw_path,
            "-dialect", "sqlite", "-sql", sql,
        ])
        run_in_container(args, image=GDAL_FULL_IMAGE, env={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
                         mem_limit=BOUNDARY_PREP_MEM_LIMIT)

        with open(prepared_path, "r", encoding="utf-8") as fh:
            prepared = json.load(fh)
        features = prepared.get("features") or []
        geometry = features[0].get("geometry") if features else None
        if geometry is None:
            print(f"[{code}] Boundary simplification produced no geometry, skipping")
            return code

        # gol's --area parser only accepts a bare GeoJSON geometry object; a
        # Feature/FeatureCollection wrapper fails with "area: Expected string".
        tmp_path = f"{prepared_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as fh:
            json.dump(geometry, fh)
        os.rename(tmp_path, prepared_path)

        minx, miny, maxx, maxy = _geometry_bbox(geometry)
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump({"bbox": [minx, miny, maxx, maxy], "geometry": geometry}, fh)
        return None
    except Exception as e:
        print(f"[{code}] Boundary preparation failed: {e}")
        return code


def build_subset_files(
    code: str,
    level_dir: str,
    boundaries_dir: str,
    snapshot_gol: str,
    snapshot_pbf: str | None = None,
) -> tuple[str, str] | None:
    """Extract PBF -> gol build (GOL) -> gol save (GOB) for one subset.

    Uses osmium's bounded-memory complete_ways extraction when snapshot_pbf is
    provided; otherwise uses gol query against snapshot_gol.

    Thread-safe. Returns None on success, ("skipped", code) when the boundary
    matches no features, ("failed", code) on error.
    """
    subset_dir = f"{level_dir}/{code}"
    pbf_path = f"{subset_dir}/{code}-latest.osm.pbf"
    gol_path = f"{subset_dir}/{code}-latest.osm.gol"
    gob_path = f"{subset_dir}/{code}-latest.osm.gob"
    marker_path = f"{subset_dir}/.built"

    # Idempotent retries: gol writes all outputs in place, so mere file
    # existence cannot distinguish complete from truncated (killed task).
    # Only the marker, written after the last step, proves completeness.
    if os.path.exists(marker_path):
        print(f"[{code}] Already built, skipping")
        return None

    try:
        os.makedirs(subset_dir, exist_ok=True)
        for stale in (pbf_path, gol_path, gob_path):
            if os.path.exists(stale):
                os.remove(stale)
        tmp_dir = f"{subset_dir}/.tmp"
        os.makedirs(tmp_dir, exist_ok=True)

        if snapshot_pbf is None:
            print(f"[{code}] gol query -> pbf")
            query = shlex.join([
                "gol", "query", snapshot_gol, "*",
                "--area", f"{boundaries_dir}/{code}.prepared.geojson",
                "-f", "pbf",
            ])
            run_in_container(f"{query} > {shlex.quote(pbf_path)}", mem_limit=GOL_MEM_LIMIT)
        else:
            # gol needs a bare GeoJSON geometry, while osmium requires a
            # Feature or FeatureCollection. Build the wrapper from the metadata
            # sidecar so both extractors use exactly the same prepared geometry.
            with open(f"{boundaries_dir}/{code}.meta.json", "r", encoding="utf-8") as fh:
                geometry = json.load(fh)["geometry"]
            osmium_boundary_path = f"{subset_dir}/{code}.osmium.geojson"
            with open(osmium_boundary_path, "w", encoding="utf-8") as fh:
                json.dump({
                    "type": "Feature",
                    "properties": {},
                    "geometry": geometry,
                }, fh)

            print(f"[{code}] osmium extract (complete_ways) -> pbf")
            run_in_container(
                [
                    "extract",
                    "--strategy", "complete_ways",
                    "--polygon", osmium_boundary_path,
                    "--set-bounds",
                    "--overwrite",
                    "--output", pbf_path,
                    snapshot_pbf,
                ],
                image=OSMIUM_IMAGE,
                mem_limit=OSMIUM_MEM_LIMIT,
                shell=False,
            )

        if os.path.getsize(pbf_path) < EMPTY_PBF_THRESHOLD_BYTES:
            print(f"[{code}] Empty extract ({os.path.getsize(pbf_path)} bytes), skipping")
            shutil.rmtree(subset_dir, ignore_errors=True)
            return ("skipped", code)

        print(f"[{code}] gol build")
        build = shlex.join(["gol", "build", "--yes", gol_path, pbf_path])
        run_in_container(build, env={"TMPDIR": tmp_dir}, mem_limit=GOL_MEM_LIMIT)

        print(f"[{code}] gol save")
        save = shlex.join(["gol", "save", gol_path, gob_path])
        run_in_container(save, mem_limit=GOL_MEM_LIMIT)

        shutil.rmtree(tmp_dir, ignore_errors=True)
        with open(marker_path, "w", encoding="utf-8") as fh:
            fh.write("built")
        return None
    except Exception as e:
        from docker.errors import ContainerError

        if isinstance(e, ContainerError):
            stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
            print(f"[{code}] Build failed (exit {e.exit_status}):\n{stderr.strip()}")
        else:
            print(f"[{code}] Build failed: {e}")
        return ("failed", code)


def parquet_copy_sql(code: str, output_path: str, boundaries_dir: str, snapshot_parquet: str) -> str:
    """Return the DuckDB COPY statement extracting one subset GeoParquet.

    Same schema as the planet file (osm_type, osm_id, tags, bbox, geometry);
    the bbox prefilter drives row-group pruning (the planet file is
    bbox-sorted), ST_Intersects against the simplified boundary decides
    membership. Insertion order is preserved so subsets stay bbox-sorted.
    """
    with open(f"{boundaries_dir}/{code}.meta.json", "r", encoding="utf-8") as fh:
        meta = json.load(fh)
    minx, miny, maxx, maxy = meta["bbox"]
    geometry_json = json.dumps(meta["geometry"]).replace("'", "''")

    return f"""
COPY (
    SELECT osm_type, osm_id, tags, bbox, geometry
    FROM read_parquet('{snapshot_parquet}')
    WHERE bbox.xmax >= {minx} AND bbox.xmin <= {maxx}
      AND bbox.ymax >= {miny} AND bbox.ymin <= {maxy}
      AND ST_Intersects(ST_GeomFromGeoJSON('{geometry_json}'), geometry)
) TO '{output_path}' (
    FORMAT PARQUET,
    CODEC 'zstd',
    COMPRESSION_LEVEL 6,
    PARQUET_VERSION v2
);
"""


def run_parquet_batch(codes: list[str], level_dir: str, boundaries_dir: str, snapshot_parquet: str, work_dir: str) -> set[str]:
    """Extract subset GeoParquet files for a batch of codes in one DuckDB session.

    Codes are processed one COPY at a time so a single failure doesn't kill the
    batch. Returns the set of failed codes.
    """
    failed: set[str] = set()
    for code in codes:
        parquet_path = f"{level_dir}/{code}/{code}-latest.osm.parquet"
        tmp_path = f"{parquet_path}.tmp"
        # Safe existence check: the final path only ever appears via the
        # rename below, so it can never be a truncated partial file.
        if os.path.exists(parquet_path):
            print(f"[{code}] Parquet already extracted, skipping")
            continue
        sql_path = f"{level_dir}/{code}/extract.sql"
        # extension_directory must be set BEFORE INSTALL: the container user
        # has no writable HOME, and the cache avoids ~3,000 re-downloads.
        with open(sql_path, "w", encoding="utf-8") as fh:
            fh.write(f"""
SET extension_directory='{work_dir}/.duckdb-extensions';
SET temp_directory='{work_dir}/.duckdb-temp';
INSTALL 'spatial'; LOAD 'spatial';
SET memory_limit='40GB';
{parquet_copy_sql(code, tmp_path, boundaries_dir, snapshot_parquet)}
""")
        try:
            print(f"[{code}] duckdb parquet extract")
            run_in_container(f"{work_dir}/duckdb -f {shlex.quote(sql_path)}", env={"HOME": work_dir})
            os.rename(tmp_path, parquet_path)
            os.remove(sql_path)
        except Exception as e:
            from docker.errors import ContainerError

            if isinstance(e, ContainerError):
                stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
                print(f"[{code}] Parquet extraction failed (exit {e.exit_status}):\n{stderr.strip()}")
            else:
                print(f"[{code}] Parquet extraction failed: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            failed.add(code)
    return failed


def upload_subset(code: str, name: str, level: str, level_dir: str, hook) -> str | None:
    """Upload the four subset files for one code using a pre-created R2IndexHook.

    Must run on the main Airflow task thread (the hook needs the task context).
    Returns the code on failure, None on success.
    """
    subset_dir = f"{level_dir}/{code}"
    try:
        for _fmt, suffix, subfolder, version, media_type, tags in SUBSET_FORMATS:
            extension = suffix.rsplit(".", 1)[-1]
            hook.upload(
                bucket=R2_BUCKET,
                category="openstreetmap",
                destination_filename=f"{code}-latest.{suffix}",
                destination_path=f"osm/{level}/{code}/{subfolder}",
                destination_version=version,
                entity=f"{code.lower()}-{subfolder}",
                extension=extension,
                media_type=media_type,
                name=name,
                source=f"{subset_dir}/{code}-latest.{suffix}",
                subcategory=level,
                tags=sorted(tags + [level, code.lower()]),
            )
        return None
    except Exception as e:
        print(f"[{code}] Upload failed: {e}")
        return code


def process_subset_batch(
    codes: list[str],
    names: dict[str, str],
    level: str,
    level_dir: str,
    boundaries_dir: str,
    snapshot_gol: str,
    snapshot_parquet: str,
    work_dir: str,
    r2index_conn_id: str,
    build_workers: int = 2,
) -> None:
    """Full pipeline for one batch: build PBF/GOL/GOB, extract parquet, upload.

    Raises AirflowException when any code fails; skipped codes (empty extracts)
    are reported but do not fail the batch. Uploaded subset outputs are removed
    to bound disk usage; a {code}.done marker records success.
    """
    from concurrent.futures import ThreadPoolExecutor

    from airflow.exceptions import AirflowException
    from elaunira.airflow.providers.r2index.hooks import R2IndexHook

    codes = [c for c in codes if not os.path.exists(f"{level_dir}/{c}.done")]
    if not codes:
        print("All codes in batch already uploaded")
        return

    with ThreadPoolExecutor(max_workers=build_workers) as executor:
        build_results = list(executor.map(
            lambda code: build_subset_files(code, level_dir, boundaries_dir, snapshot_gol),
            codes,
        ))

    results = [r for r in build_results if r is not None]
    failed = {code for status, code in results if status == "failed"}
    skipped = {code for status, code in results if status == "skipped"}
    if skipped:
        print(f"Skipped {len(skipped)} empty extract(s): {sorted(skipped)}")

    parquet_codes = [c for c in codes if c not in failed and c not in skipped]
    failed |= run_parquet_batch(parquet_codes, level_dir, boundaries_dir, snapshot_parquet, work_dir)

    hook = R2IndexHook(r2index_conn_id=r2index_conn_id)
    for code in codes:
        if code in failed or code in skipped:
            continue
        if upload_subset(code, names.get(code, code), level, level_dir, hook) is None:
            with open(f"{level_dir}/{code}.done", "w", encoding="utf-8") as fh:
                fh.write("uploaded")
            shutil.rmtree(f"{level_dir}/{code}", ignore_errors=True)
        else:
            failed.add(code)

    if failed:
        raise AirflowException(f"{len(failed)}/{len(codes)} subset(s) failed: {sorted(failed)}")
