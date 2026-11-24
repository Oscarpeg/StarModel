"""
duck_engine.py - Motor local para probar star_etl.py SIN AWS.

Ejecuta exactamente los mismos SELECT del job (escritos en SQL de Athena/Trino), traducidos a DuckDB
con sqlglot. La landing zone es una carpeta local y los CSV de referencia salen de generate_p3.
Solo sirve para validar la logica contra el oraculo del harness antes de gastar en AWS.

Variables de entorno: STAR_ENGINE=duckdb, STAR_DUCKDB_PATH, STAR_LOCAL_LANDING, STAR_LOCAL_REF
"""
import glob
import os
import time

import duckdb
import sqlglot

import star_etl


_UNNEST_TRINO = "CROSS JOIN UNNEST(REGEXP_EXTRACT_ALL(r.line, '\\{[^{}]*\\}')) AS u(obj)"


def to_duckdb(sql):
    out = sqlglot.transpile(sql, read="trino", write="duckdb")[0]
    # DuckDB, a diferencia de Trino/Athena, copia la linea completa (hasta ~1 MB) por cada objeto del
    # CROSS JOIN UNNEST y se queda sin memoria. Solo aqui lo reescribimos con el UNNEST en el SELECT.
    if _UNNEST_TRINO in out:
        out = out.replace(
            "SELECT r.\"$path\" AS src_file, u.obj FROM raw_events_text AS r " + _UNNEST_TRINO,
            "SELECT src_file, obj FROM (SELECT r.\"$path\" AS src_file, "
            "UNNEST(REGEXP_EXTRACT_ALL(r.line, '\\{[^{}]*\\}')) AS obj FROM raw_events_text AS r) AS u")
        assert _UNNEST_TRINO not in out, "no se pudo reescribir el UNNEST para DuckDB"
    return out


class DuckEngine:
    name = "duckdb"

    def __init__(self, db_path, landing_dir, ref_dir):
        self.db_path, self.landing, self.ref = db_path, landing_dir, ref_dir
        self.cfg = {"database": "local"}

    @classmethod
    def from_env(cls):
        return cls(os.environ["STAR_DUCKDB_PATH"], os.environ["STAR_LOCAL_LANDING"], os.environ["STAR_LOCAL_REF"])

    def _con(self):
        return duckdb.connect(self.db_path)

    def ensure_sources(self):
        con = self._con()
        try:
            files = [p for p in glob.glob(os.path.join(self.landing, "**", "*"), recursive=True) if os.path.isfile(p)]
            if files:
                # una sola columna con la linea completa; \x01 nunca aparece en JSON (igual que en Athena)
                con.execute(f"""
                    CREATE OR REPLACE VIEW raw_events_text AS
                    SELECT column0 AS line, filename AS "$path"
                    FROM read_csv('{self.landing}/**/*', columns={{'column0': 'VARCHAR'}}, header=false,
                                  delim='\x01', quote='', escape='', auto_detect=false, filename=true,
                                  max_line_size=16777216, buffer_size=33554432, compression='auto', strict_mode=false)""")
            else:
                con.execute("""CREATE OR REPLACE VIEW raw_events_text AS
                               SELECT CAST(NULL AS VARCHAR) AS line, CAST(NULL AS VARCHAR) AS "$path" WHERE false""")
            for name, fname, cols in (
                    ("ref_routes", "routes_ref.csv", "route, origin, destination, region"),
                    ("ref_route_category_history", "route_category_history.csv", "route, route_category, effective_date"),
                    ("ref_holidays", "holidays_ref.csv", "holiday_date, holiday_name")):
                spec = ", ".join(f"'{c.strip()}': 'VARCHAR'" for c in cols.split(","))
                con.execute(f"""CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_csv(
                    '{os.path.join(self.ref, fname)}', header=true, columns={{{spec}}}, auto_detect=false)""")
        finally:
            con.close()
        return {}

    def ctas(self, table, select_sql, partitioned_by, run_id):
        t0 = time.time()
        con = self._con()
        try:
            con.execute(f"CREATE OR REPLACE TABLE {table} AS {to_duckdb(star_etl._fill(select_sql))}")
        finally:
            con.close()
        return {"bytes_scanned": 0, "seconds": round(time.time() - t0, 2)}

    def rows(self, sql):
        con = self._con()
        try:
            cur = con.execute(to_duckdb(sql))
            return [d[0] for d in cur.description], cur.fetchall()
        finally:
            con.close()

    def checks(self, sql):
        header, rows = self.rows(sql)
        return {h: (int(v) if v is not None else None) for h, v in zip(header, rows[0])}

    def cleanup(self, run_id):
        return {"old_objects_deleted": 0}

    def reset(self, landing=False):
        con = self._con()
        try:
            for table, _, _ in star_etl.PLAN:
                con.execute(f"DROP TABLE IF EXISTS {table}")
        finally:
            con.close()
        out = {"star_objects_deleted": 0}
        if landing:
            n = 0
            for p in glob.glob(os.path.join(self.landing, "**", "*"), recursive=True):
                if os.path.isfile(p):
                    os.remove(p)
                    n += 1
            out["landing_objects_deleted"] = n
        return out
