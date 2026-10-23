#!/usr/bin/env python3
"""Replace cricsocc.bronze with the local Postgres archive snapshot."""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from databricks import sql

ROOT = Path(__file__).resolve().parents[1]
EXPORT_ROOT = Path(os.environ.get("TEMP", ".")) / "cricsocc_export"
TABLES = [
    "teams",
    "players",
    "series",
    "series_years",
    "venues",
    "matches",
    "match_facts",
    "awards",
    "squads",
    "innings",
    "extras",
    "batting",
    "bowling",
    "wickets",
    "partnerships",
    "powerplays",
    "commentary",
]
CHUNK_ROWS = {
    "commentary": 20000,
    "squads": 50000,
    "batting": 50000,
    "match_facts": 50000,
    "wickets": 50000,
    "bowling": 50000,
    "partnerships": 50000,
}
ARROW = {
    "int2": pa.int16(),
    "int4": pa.int32(),
    "int8": pa.int64(),
    "float4": pa.float32(),
    "float8": pa.float64(),
    "bool": pa.bool_(),
    "text": pa.string(),
    "timestamptz": pa.timestamp("us"),
}
SPARK = {
    "int2": "SMALLINT",
    "int4": "INT",
    "int8": "BIGINT",
    "float4": "FLOAT",
    "float8": "DOUBLE",
    "bool": "BOOLEAN",
    "text": "STRING",
    "timestamptz": "TIMESTAMP_NTZ",
}


def load_env() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def spark_run(cursor, statement: str) -> None:
    print(statement.split("\n", 1)[0][:180], flush=True)
    cursor.execute(statement)
    try:
        cursor.fetchall()
    except Exception:
        return


def columns(pg) -> dict[str, list[tuple[str, str, str]]]:
    found: dict[str, list[tuple[str, str, str]]] = {table: [] for table in TABLES}
    rows = pg.execute(
        """
        SELECT table_name, column_name, udt_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'archive' AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
        """,
        (TABLES,),
    ).fetchall()
    for table, column, udt, nullable in rows:
        found[table].append((column, udt, nullable))
    missing = [table for table, cols in found.items() if not cols]
    if missing:
        raise SystemExit(f"Missing Postgres tables: {missing}")
    return found


def convert(value, udt):
    if value is None or udt != "timestamptz":
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def export_table(pg, table: str, spec: list[tuple[str, str, str]]) -> tuple[int, list[Path]]:
    names = [column for column, _, _ in spec]
    schema = pa.schema([(column, ARROW[udt]) for column, udt, _ in spec])
    folder = EXPORT_ROOT / table
    folder.mkdir(parents=True, exist_ok=True)
    for old in folder.glob("*.parquet"):
        old.unlink()
    chunk = CHUNK_ROWS.get(table, 100000)
    sql_text = "SELECT " + ", ".join(names) + f" FROM archive.{table}"
    count = 0
    part = 0
    paths: list[Path] = []
    with pg.cursor(name=f"export_{table}") as cursor:
        cursor.itersize = chunk
        cursor.execute(sql_text)
        while True:
            rows = cursor.fetchmany(chunk)
            if not rows:
                break
            records = [
                {name: convert(value, udt) for (name, udt, _), value in zip(spec, row)}
                for row in rows
            ]
            path = folder / f"part-{part:05d}.parquet"
            pq.write_table(pa.Table.from_pylist(records, schema=schema), path, compression="snappy")
            count += len(records)
            paths.append(path)
            part += 1
            print(f"export {table} rows {count} file {path.name} bytes {path.stat().st_size}", flush=True)
    print(f"export {table} complete {count}", flush=True)
    return count, paths


def upload(path: Path, remote: str) -> None:
    host = os.environ["DATABRICKS_SERVER_HOSTNAME"]
    token = os.environ["DATABRICKS_TOKEN"]
    url = f"https://{host}/api/2.0/fs/files{remote}"
    with path.open("rb") as handle:
        response = requests.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            data=handle,
            timeout=1800,
        )
    if response.status_code not in (200, 204):
        raise RuntimeError(f"Upload {path.name} failed HTTP {response.status_code}: {response.text[:400]}")
    print(f"uploaded {remote} bytes {path.stat().st_size}", flush=True)


def create_table(cursor, table: str, spec: list[tuple[str, str, str]]) -> None:
    body = ",\n".join(
        f"  `{column}` {SPARK[udt]}" + ("" if nullable == "YES" else " NOT NULL")
        for column, udt, nullable in spec
    )
    spark_run(cursor, f"CREATE TABLE cricsocc.bronze.{table} (\n{body}\n) USING DELTA")


def main() -> None:
    load_env()
    EXPORT_ROOT.mkdir(parents=True, exist_ok=True)
    manifest_path = EXPORT_ROOT / "counts.json"
    saved = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    pg = psycopg.connect(os.environ["ARCHIVE_DATABASE_URL"])
    spec = columns(pg)
    expected: dict[str, int] = {}
    pending = []
    for table in TABLES:
        parts = list((EXPORT_ROOT / table).glob("*.parquet"))
        if parts and table in saved:
            expected[table] = int(saved[table])
            print(f"reuse {table} files {len(parts)} rows {expected[table]}", flush=True)
        else:
            pending.append(table)
    if pending:
        pg.commit()
        with pg.transaction():
            pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            pg.execute("SET TRANSACTION READ ONLY")
            for table in pending:
                count, paths = export_table(pg, table, spec[table])
                expected[table] = count
                if count and not paths:
                    raise SystemExit(f"{table} exported no files for {count} rows")
    pg.close()
    manifest_path.write_text(json.dumps(expected), encoding="utf-8")
    print("snapshot ready", expected, flush=True)
    connection = sql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )
    cursor = connection.cursor()
    spark_run(cursor, "DROP SCHEMA IF EXISTS cricsocc.bronze CASCADE")
    spark_run(cursor, "CREATE SCHEMA cricsocc.bronze")
    spark_run(cursor, "CREATE VOLUME cricsocc.bronze.landing")
    for table in TABLES:
        create_table(cursor, table, spec[table])
        folder = EXPORT_ROOT / table
        parts = sorted(folder.glob("*.parquet"))
        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = [
                pool.submit(upload, path, f"/Volumes/cricsocc/bronze/landing/{table}/{path.name}")
                for path in parts
            ]
            for job in as_completed(jobs):
                job.result()
        if parts:
            spark_run(
                cursor,
                f"""
                COPY INTO cricsocc.bronze.{table}
                FROM '/Volumes/cricsocc/bronze/landing/{table}'
                FILEFORMAT = PARQUET
                COPY_OPTIONS ('force' = 'true')
                """,
            )
        cursor.execute(f"SELECT COUNT(*) FROM cricsocc.bronze.{table}")
        loaded = int(cursor.fetchone()[0])
        if loaded != expected[table]:
            raise SystemExit(f"{table} loaded {loaded}, snapshot has {expected[table]}")
        print(f"verified {table} {loaded}", flush=True)
    for table in TABLES:
        for path in (EXPORT_ROOT / table).glob("*.parquet"):
            path.unlink()
    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()
