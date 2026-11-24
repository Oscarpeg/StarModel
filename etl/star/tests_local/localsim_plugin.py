"""
localsim_plugin.py - Plugin de pytest que reemplaza AWS por una simulacion local para correr los
tests OFICIALES del harness (tests/test_2_star.py: R5-R10) contra star_etl.py + star.py.

  * kinesis_put  -> simula la entrega de la compañera a la landing zone: archivos estilo Firehose,
                    JSON pegados SIN salto de linea y comprimidos .gz (el peor caso), mas algunos en
                    JSON por linea.
  * athena_run   -> traduce el SQL del harness (Trino) a DuckDB y lo corre sobre las tablas del ETL.
  * glue_*       -> describe las tablas de DuckDB como si fueran el Glue Data Catalog.

Se usa por medio de run_local.py (que fija SIM_DIR, SIM_SEED, SIM_SALT y SIM_BASE_URL).
"""
import datetime as dt
import glob
import gzip
import os
import uuid

import duckdb
import sqlglot
import yaml

from harness import registry

SIM_DIR = os.environ["SIM_DIR"]
LANDING = os.path.join(SIM_DIR, "landing")
DB_PATH = os.path.join(SIM_DIR, "star.duckdb")
TYPE_MAP = {"INTEGER": "int", "BIGINT": "bigint", "SMALLINT": "smallint", "TINYINT": "tinyint",
            "VARCHAR": "string", "DATE": "date", "BOOLEAN": "boolean", "DOUBLE": "double",
            "TIMESTAMP": "timestamp", "HUGEINT": "bigint"}
PARTITIONS = {"fact_search": "month"}


def _fmt(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()
    return str(v)


class FakeAWS:
    _seq = 0

    # ---------------------------------------------------------------- landing zone (lado de la compañera)
    def kinesis_put(self, stream, records):
        now = dt.datetime.now(dt.timezone.utc)
        chunk = 4000
        for i in range(0, len(records), chunk):
            FakeAWS._seq += 1
            part = records[i:i + chunk]
            ts = now + dt.timedelta(seconds=FakeAWS._seq)
            folder = os.path.join(LANDING, ts.strftime("%Y/%m/%d/%H"))
            os.makedirs(folder, exist_ok=True)
            name = f"airline-events-1-{ts.strftime('%Y-%m-%d-%H-%M-%S')}-{uuid.uuid4()}"
            if FakeAWS._seq % 3:   # 2 de cada 3 archivos: Firehose sin delimitador + GZIP
                with gzip.open(os.path.join(folder, name + ".gz"), "wb") as fh:
                    fh.write(b"".join(d for _, d in part))
            else:                  # 1 de cada 3: JSON por linea sin comprimir
                with open(os.path.join(folder, name + ".json"), "wb") as fh:
                    fh.write(b"\n".join(d for _, d in part) + b"\n")

    def s3_read_text_since(self, uri, since):
        out = []
        for p in glob.glob(os.path.join(LANDING, "**", "*"), recursive=True):
            if not os.path.isfile(p):
                continue
            if dt.datetime.fromtimestamp(os.path.getmtime(p), dt.timezone.utc) < since:
                continue
            body = open(p, "rb").read()
            if body[:2] == b"\x1f\x8b":
                body = gzip.decompress(body)
            out.append((p, body.decode("utf-8", errors="replace")))
        return out

    # ---------------------------------------------------------------------------- Athena / Glue
    def athena_run(self, sql, database, workgroup, timeout=180):
        q = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            rows = [[_fmt(v) for v in r] for r in con.execute(q).fetchall()]
            scanned = 0
            if "fact_search" in sql:   # aproximacion: Athena solo lee las particiones del filtro
                where = "WHERE month = '2025-01'" if "month = '2025-01'" in sql else ""
                scanned = 100 * con.execute(f"SELECT count(*) FROM fact_search {where}").fetchone()[0]
        finally:
            con.close()
        return {"execution_id": "local", "rows": rows, "data_scanned_bytes": scanned}

    def glue_get_table(self, database, name):
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                                 [name]).fetchone()[0]
            if not exists:
                return None
            cols = [(r[0], TYPE_MAP.get(r[1], r[1].lower())) for r in con.execute(f"DESCRIBE {name}").fetchall()]
        finally:
            con.close()
        pk = PARTITIONS.get(name)
        return {"name": name,
                "columns": [{"name": c, "type": t} for c, t in cols if c != pk],
                "partition_keys": [{"name": pk, "type": "string"}] if pk else [],
                "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "serde": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "location": "local", "classification": "parquet", "projection_enabled": False}

    def glue_partition_count(self, database, name):
        con = duckdb.connect(DB_PATH, read_only=True)
        try:
            return con.execute(f"SELECT count(DISTINCT {PARTITIONS[name]}) FROM {name}").fetchone()[0]
        finally:
            con.close()

    def athena_workgroup(self, workgroup):
        return {"bytes_cutoff": 1_000_000_000}

    def kinesis_summary(self, stream):
        return {"mode": "PROVISIONED", "open_shards": 1, "retention_hours": 24, "consumers": 0}


registry.AWS_OVERRIDE = FakeAWS()
registry.CONFIG_OVERRIDE = {
    "team_id": "local-sim", "seed": int(os.environ["SIM_SEED"]),
    "backend": {"base_url": os.environ["SIM_BASE_URL"]},
    "aws": {"region": "us-east-1", "kinesis_stream": "airline-events", "raw_events_uri": "s3://sim/raw/events/",
            "athena_workgroup": "airline-star", "athena_database": "airline_star"},
    "cost_projection_file": "cost_projection.yaml",
}
_thr = yaml.safe_load(open(os.path.join(os.environ["SIM_HARNESS"], "thresholds.yaml"), encoding="utf-8"))
_thr["probe_salt"] = int(os.environ.get("SIM_SALT", "0"))
registry.THRESHOLDS_OVERRIDE = _thr
