"""
star_etl.py - Job de carga del modelo estrella (Parcial 3, Persona B).

Flujo completo, todo en Athena (sin cluster ni servidores extra):

    landing zone (JSON crudo en S3, append-only, puede traer duplicados)
        |
        v
    stg_events          eventos parseados, tipados, deduplicados por event_id, con motivo de rechazo
        |
        +--> dim_date            calendario 2023-2027 enriquecido (trimestre, dia, festivos, puentes)
        +--> dim_route           SCD tipo 2: una fila por version de la categoria de la ruta
        +--> dim_fare_class      clases tarifarias
        +--> fact_search         1 fila por busqueda distinta, particionada por mes (YYYY-MM)
        +--> fact_booking_event  1 fila por reserva/cancelacion distinta

Cada corrida reconstruye las tablas con CTAS en una carpeta nueva de S3 (v=<run_id>). Por eso
correrla dos veces con la misma entrada da exactamente el mismo resultado (idempotente), y los
eventos tardios o desordenados quedan igual que si hubieran llegado a tiempo.

Uso (desde CloudShell o la EC2, con las variables de entorno de abajo):
    python star_etl.py setup --ref-dir ref/   # una vez: workgroup con limite 1 GB, base, CSV de referencia
    python star_etl.py run                    # una corrida completa (lo mismo que POST /admin/refresh-star)
    python star_etl.py checks                 # conteos de integridad del modelo
    python star_etl.py reset --landing --yes  # antes de la sustentacion: vacia landing zone y star

Variables de entorno:
    STAR_BUCKET      bucket del equipo (obligatoria). De ahi salen s3://<bucket>/star/, /ref/, /athena-results/
    RAW_EVENTS_URI   landing zone, ej. s3://<bucket>/raw/events/ (obligatoria; la misma de config.yaml)
    STAR_DATABASE    base en Glue/Athena (default airline_star)
    STAR_WORKGROUP   workgroup de Athena (default airline-star; el mismo de config.yaml)
    AWS_REGION       default us-east-1
"""
import argparse
import datetime as dt
import json
import os
import sys
import time
from urllib.parse import urlparse

CALENDAR_START = "2023-01-01"
CALENDAR_END = "2027-12-31"
FAR_FUTURE = "9999-12-31"
SCAN_LIMIT_BYTES = 1_000_000_000   # R10: el test exige <= 1.000.000.000 bytes (ojo: "1 GB" en la consola son 1.073.741.824)

# Tablas que construye el job, en orden. (tabla, funcion que arma el SELECT, columnas de particion)
# Las dimensiones van antes que los hechos porque los hechos buscan sus llaves en ellas.
STAR_TABLES = ["dim_date", "dim_route", "dim_fare_class", "fact_search", "fact_booking_event"]


# --------------------------------------------------------------------------------------- config
def _dir(uri):
    return uri if uri.endswith("/") else uri + "/"


def load_config(env=None):
    env = os.environ if env is None else env
    bucket = env.get("STAR_BUCKET")
    raw = env.get("RAW_EVENTS_URI")
    if not bucket or not raw:
        raise RuntimeError("Faltan las variables de entorno STAR_BUCKET y/o RAW_EVENTS_URI")
    return {
        "region": env.get("AWS_REGION", env.get("AWS_DEFAULT_REGION", "us-east-1")),
        "database": env.get("STAR_DATABASE", "airline_star"),
        "workgroup": env.get("STAR_WORKGROUP", "airline-star"),
        "bucket": bucket,
        "raw_uri": _dir(raw),
        "star_uri": _dir(env.get("STAR_S3_URI", f"s3://{bucket}/star/")),
        "ref_uri": _dir(env.get("REF_S3_URI", f"s3://{bucket}/ref/")),
        "athena_output": env.get("STAR_ATHENA_OUTPUT", f"s3://{bucket}/athena-results/"),
    }


def _fill(sql):
    """Los SQL usan marcadores @START@/@END@ en vez de f-strings para que las regex y los
    '$path' / '$.campo' se lean tal cual."""
    return sql.replace("@START@", CALENDAR_START).replace("@END@", CALENDAR_END).replace("@FAR@", FAR_FUTURE)


# ------------------------------------------------------------------------- fuentes (tablas externas)
def source_ddls(cfg):
    """Tablas externas sobre S3. Se borran y recrean en cada corrida (DROP de una tabla externa no
    borra datos y el DDL no se cobra), asi siempre apuntan a las rutas de la configuracion actual."""
    ref = cfg["ref_uri"]
    csv = ("ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '{loc}' "
           "TBLPROPERTIES ('skip.header.line.count'='1')")
    return {
        # La landing zone se lee como TEXTO (una columna con la linea completa) y no con el SerDe de
        # JSON: asi funciona igual con JSON por linea, con JSON pegados sin salto de linea (Firehose
        # sin delimitador, o el replay del harness) y con .gz.
        "raw_events_text": f"CREATE EXTERNAL TABLE raw_events_text (line string) "
                           f"STORED AS TEXTFILE LOCATION '{cfg['raw_uri']}'",
        "ref_routes": "CREATE EXTERNAL TABLE ref_routes (route string, origin string, destination string, "
                      "region string) " + csv.format(loc=ref + "routes/"),
        "ref_route_category_history": "CREATE EXTERNAL TABLE ref_route_category_history (route string, "
                                      "route_category string, effective_date string) "
                                      + csv.format(loc=ref + "route_category_history/"),
        "ref_holidays": "CREATE EXTERNAL TABLE ref_holidays (holiday_date string, holiday_name string) "
                        + csv.format(loc=ref + "holidays/"),
    }


# ------------------------------------------------------------------------------------- staging
SQL_STG_EVENTS = r"""
WITH objs AS (
    -- 1) Una linea puede traer uno o varios JSON pegados: sacamos cada objeto plano {...} con regex.
    SELECT r."$path" AS src_file, u.obj
    FROM raw_events_text r
    CROSS JOIN UNNEST(regexp_extract_all(r.line, '\{[^{}]*\}')) AS u(obj)
),
parsed AS (
    -- 2) Parseo y normalizacion de cada campo del contrato (TRY_CAST: un valor malo da NULL, no rompe el job).
    SELECT
        src_file,
        NULLIF(trim(json_extract_scalar(obj, '$.event_id')), '')            AS event_id,
        lower(trim(json_extract_scalar(obj, '$.event_type')))               AS event_type,
        trim(json_extract_scalar(obj, '$.event_ts'))                        AS event_ts,
        TRY_CAST(json_extract_scalar(obj, '$.schema_version') AS INTEGER)   AS schema_version,
        json_extract_scalar(obj, '$.session_id')                            AS session_id,
        upper(trim(json_extract_scalar(obj, '$.origin')))                   AS origin,
        upper(trim(json_extract_scalar(obj, '$.destination')))              AS destination,
        TRY_CAST(json_extract_scalar(obj, '$.travel_date') AS DATE)         AS travel_date,
        TRY_CAST(json_extract_scalar(obj, '$.results_count') AS INTEGER)    AS results_count,
        json_extract_scalar(obj, '$.reservation_id')                        AS reservation_id,
        json_extract_scalar(obj, '$.flight_id')                             AS flight_id,
        upper(regexp_replace(json_extract_scalar(obj, '$.route'), '\s', '')) AS booking_route,
        TRY_CAST(json_extract_scalar(obj, '$.flight_date') AS DATE)         AS flight_date,
        upper(trim(json_extract_scalar(obj, '$.fare_class')))               AS fare_class,
        TRY_CAST(json_extract_scalar(obj, '$.price_cents') AS BIGINT)       AS price_cents
    FROM objs
),
ranked AS (
    -- 3) Deduplicacion por event_id: queda UNA copia por evento (la del archivo mas reciente; los
    --    nombres de archivo de la landing zone van en orden de llegada).
    SELECT p.*,
           row_number() OVER (PARTITION BY event_id ORDER BY src_file DESC) AS copy_rank,
           count(*)     OVER (PARTITION BY event_id)                        AS copies
    FROM parsed p
    WHERE event_id IS NOT NULL
    UNION ALL
    -- sin event_id no se puede deduplicar: se guardan todos, marcados como rechazados
    SELECT p.*, CAST(1 AS BIGINT) AS copy_rank, CAST(1 AS BIGINT) AS copies
    FROM parsed p
    WHERE event_id IS NULL
),
clean AS (
    -- 4) La fecha sale de event_ts (cuando paso), nunca de cuando llego: asi un evento tardio cae en su dia.
    SELECT ranked.*, TRY_CAST(substr(event_ts, 1, 10) AS DATE) AS event_date
    FROM ranked
    WHERE copy_rank = 1
)
SELECT
    event_id,
    event_type,
    event_ts,
    event_date,
    CAST(year(event_date) * 10000 + month(event_date) * 100 + day(event_date) AS INTEGER) AS date_key,
    substr(event_ts, 1, 7) AS month,
    CASE WHEN event_type = 'search_performed' THEN origin || '-' || destination ELSE booking_route END AS route,
    schema_version,
    session_id,
    origin,
    destination,
    travel_date,
    results_count,
    reservation_id,
    flight_id,
    flight_date,
    fare_class,
    price_cents,
    CAST(copies AS INTEGER) AS copies,
    src_file,
    CASE
        WHEN event_id IS NULL
            THEN 'missing_event_id'
        WHEN event_type IS NULL
          OR event_type NOT IN ('search_performed', 'reservation_created', 'reservation_cancelled')
            THEN 'unknown_event_type'
        WHEN event_ts IS NULL
          OR NOT regexp_like(event_ts, '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$')
          OR event_date IS NULL
            THEN 'bad_event_ts'
        WHEN event_date < DATE '@START@' OR event_date > DATE '@END@'
            THEN 'outside_calendar'
        WHEN event_type <> 'search_performed' AND price_cents IS NULL
            THEN 'missing_price'
        ELSE NULL
    END AS reject_reason
FROM clean
"""

# ------------------------------------------------------------------------------------ dimensiones
SQL_DIM_DATE = r"""
WITH days AS (
    SELECT CAST(d AS DATE) AS d
    FROM UNNEST(sequence(DATE '@START@', DATE '@END@', INTERVAL '1' DAY)) AS t(d)
),
hol AS (
    -- festivos colombianos de holidays_ref.csv (agrupado por si una fecha viniera repetida)
    SELECT CAST(trim(holiday_date) AS DATE) AS hd, max(trim(holiday_name)) AS holiday_name
    FROM ref_holidays
    GROUP BY 1
),
cal AS (
    -- day_of_week: 1 = lunes ... 7 = domingo (ISO).
    -- Para sabado y domingo calculamos el viernes y el lunes de ESE fin de semana.
    SELECT d,
           day_of_week(d) AS dow,
           CASE day_of_week(d) WHEN 6 THEN CAST(date_add('day', -1, d) AS DATE)
                               WHEN 7 THEN CAST(date_add('day', -2, d) AS DATE) END AS wk_friday,
           CASE day_of_week(d) WHEN 6 THEN CAST(date_add('day', 2, d) AS DATE)
                               WHEN 7 THEN CAST(date_add('day', 1, d) AS DATE) END AS wk_monday
    FROM days
)
SELECT
    CAST(year(c.d) * 10000 + month(c.d) * 100 + day(c.d) AS INTEGER) AS date_key,
    c.d                                   AS full_date,
    CAST(year(c.d) AS INTEGER)            AS year,
    CAST(quarter(c.d) AS INTEGER)         AS quarter,
    CAST(month(c.d) AS INTEGER)           AS month,
    CAST(c.dow AS INTEGER)                AS weekday,
    CASE c.dow WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday'
               WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday' ELSE 'Sunday' END AS weekday_name,
    c.dow >= 6                            AS is_weekend,
    h.hd IS NOT NULL                      AS is_holiday,
    COALESCE(h.holiday_name, '')          AS holiday_name,
    -- Puente: festivo en viernes o lunes -> ese dia + el sabado y el domingo de en medio.
    CASE
        WHEN c.dow IN (1, 5) THEN h.hd IS NOT NULL
        WHEN c.dow IN (6, 7) THEN (hf.hd IS NOT NULL OR hm.hd IS NOT NULL)
        ELSE false
    END                                   AS is_long_weekend,
    -- atributos extra (el contrato los permite): reportes mensuales legibles y dias habiles
    CAST(day(c.d) AS INTEGER)             AS day_of_month,
    substr(CAST(c.d AS VARCHAR), 1, 7)    AS year_month,
    CASE month(c.d) WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March' WHEN 4 THEN 'April'
                    WHEN 5 THEN 'May' WHEN 6 THEN 'June' WHEN 7 THEN 'July' WHEN 8 THEN 'August'
                    WHEN 9 THEN 'September' WHEN 10 THEN 'October' WHEN 11 THEN 'November'
                    ELSE 'December' END   AS month_name,
    (c.dow < 6 AND h.hd IS NULL)          AS is_business_day
FROM cal c
LEFT JOIN hol h  ON h.hd  = c.d
LEFT JOIN hol hf ON hf.hd = c.wk_friday
LEFT JOIN hol hm ON hm.hd = c.wk_monday
"""

SQL_DIM_ROUTE = r"""
WITH routes AS (
    SELECT upper(trim(route)) AS route, upper(trim(origin)) AS origin,
           upper(trim(destination)) AS destination, upper(trim(region)) AS region
    FROM ref_routes
),
hist AS (
    SELECT upper(trim(route)) AS route, upper(trim(route_category)) AS route_category,
           CAST(trim(effective_date) AS DATE) AS valid_from
    FROM ref_route_category_history
),
versions AS (
    -- SCD tipo 2: cada cambio de categoria abre una version nueva. valid_to (exclusivo) es el
    -- valid_from de la version siguiente; la vigente queda hasta 9999-12-31.
    SELECT route, route_category, valid_from,
           COALESCE(lead(valid_from) OVER (PARTITION BY route ORDER BY valid_from), DATE '@FAR@') AS valid_to,
           row_number() OVER (PARTITION BY route ORDER BY valid_from) AS route_version
    FROM hist
)
SELECT
    CAST(row_number() OVER (ORDER BY v.route, v.valid_from) AS INTEGER) AS route_key,
    v.route,
    r.origin,
    r.destination,
    r.region,
    v.route_category,
    v.valid_from,
    v.valid_to,
    v.valid_to = DATE '@FAR@'             AS is_current,
    CAST(v.route_version AS INTEGER)      AS route_version
FROM versions v
JOIN routes r ON r.route = v.route
UNION ALL
-- Miembro "desconocido" (llave -1): hechos con una ruta que no esta en routes_ref (p. ej. una
-- busqueda BOG-XYZ desde el frontend). Asi no se pierden y no quedan huerfanos (R6).
SELECT CAST(-1 AS INTEGER), 'UNKNOWN', 'UNK', 'UNK', 'UNKNOWN', 'UNKNOWN',
       DATE '1900-01-01', DATE '@FAR@', true, CAST(1 AS INTEGER)
"""

SQL_DIM_FARE_CLASS = r"""
SELECT CAST(fare_class_key AS INTEGER) AS fare_class_key,
       fare_class,
       cabin_es,
       CAST(cabin_rank AS INTEGER) AS cabin_rank
FROM (VALUES
    (1, 'ECONOMY', 'Economica', 1),
    (2, 'BUSINESS', 'Ejecutiva', 2),
    (-1, 'UNKNOWN', 'Desconocida', 9)
) AS t(fare_class_key, fare_class, cabin_es, cabin_rank)
"""

# ----------------------------------------------------------------------------------------- hechos
# "As-of join": la version de la ruta es la vigente en la FECHA DEL EVENTO (valid_from <= fecha < valid_to),
# no la vigente hoy ni la del dia en que llego el evento.
SQL_FACT_SEARCH = r"""
SELECT
    e.event_id,
    e.date_key,
    COALESCE(r.route_key, -1) AS route_key,
    e.results_count,
    1                         AS searches,
    e.travel_date,
    e.event_ts,
    e.month
FROM stg_events e
LEFT JOIN dim_route r
       ON r.route = e.route
      AND e.event_date >= r.valid_from
      AND e.event_date <  r.valid_to
WHERE e.reject_reason IS NULL
  AND e.event_type = 'search_performed'
"""

SQL_FACT_BOOKING_EVENT = r"""
SELECT
    e.event_id,
    e.date_key,
    COALESCE(r.route_key, -1)       AS route_key,
    COALESCE(fc.fare_class_key, -1) AS fare_class_key,
    e.reservation_id,
    e.event_type,
    CASE WHEN e.event_type = 'reservation_created'   THEN 1 ELSE 0 END AS bookings,
    CASE WHEN e.event_type = 'reservation_cancelled' THEN 1 ELSE 0 END AS cancellations,
    CASE WHEN e.event_type = 'reservation_created' THEN e.price_cents ELSE -e.price_cents END AS revenue_cents,
    e.price_cents,
    e.flight_id,
    e.flight_date,
    e.event_ts
FROM stg_events e
LEFT JOIN dim_route r
       ON r.route = e.route
      AND e.event_date >= r.valid_from
      AND e.event_date <  r.valid_to
LEFT JOIN dim_fare_class fc
       ON fc.fare_class = e.fare_class
WHERE e.reject_reason IS NULL
  AND e.event_type IN ('reservation_created', 'reservation_cancelled')
"""

SQL_CHECKS = r"""
SELECT
    (SELECT count(*) FROM dim_date)                                                 AS dim_date_rows,
    (SELECT count(*) - count(DISTINCT route_key) FROM dim_route)                    AS dim_route_duplicate_keys,
    (SELECT count(*) FROM fact_search)                                              AS fact_search_rows,
    (SELECT count(*) - count(DISTINCT event_id) FROM fact_search)                   AS fact_search_duplicate_events,
    (SELECT count(*) FROM fact_booking_event)                                       AS fact_booking_rows,
    (SELECT count(*) - count(DISTINCT event_id) FROM fact_booking_event)            AS fact_booking_duplicate_events,
    (SELECT count(*) FROM fact_search WHERE route_key = -1)                         AS search_unknown_route,
    (SELECT count(*) FROM fact_booking_event WHERE route_key = -1 OR fare_class_key = -1) AS booking_unknown_member,
    (SELECT count(*) FROM stg_events WHERE reject_reason IS NOT NULL)               AS rejected_events,
    (SELECT COALESCE(sum(copies - 1), 0) FROM stg_events)                           AS duplicate_copies_removed
"""

PLAN = [
    ("stg_events", SQL_STG_EVENTS, None),
    ("dim_date", SQL_DIM_DATE, None),
    ("dim_route", SQL_DIM_ROUTE, None),
    ("dim_fare_class", SQL_DIM_FARE_CLASS, None),
    ("fact_search", SQL_FACT_SEARCH, ["month"]),
    ("fact_booking_event", SQL_FACT_BOOKING_EVENT, None),
]


# ----------------------------------------------------------------------------------------- Athena
def split_s3(uri):
    u = urlparse(uri)
    return u.netloc, u.path.lstrip("/")


class AthenaEngine:
    """Ejecuta el plan en Athena. La EC2 solo manda SQL y espera; el trabajo lo hace Athena."""
    name = "athena"

    def __init__(self, cfg):
        import boto3
        self.cfg = cfg
        s = boto3.Session(region_name=cfg["region"])
        self.athena = s.client("athena")
        self.s3 = s.client("s3")

    # -- bajo nivel
    def execute(self, sql, label="", use_database=True, timeout=600):
        kw = {"QueryString": sql, "WorkGroup": self.cfg["workgroup"]}
        if use_database:
            kw["QueryExecutionContext"] = {"Database": self.cfg["database"]}
        if self.cfg.get("athena_output"):
            kw["ResultConfiguration"] = {"OutputLocation": self.cfg["athena_output"]}
        qid = self.athena.start_query_execution(**kw)["QueryExecutionId"]
        t0, pause = time.time(), 0.5
        while True:
            qe = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = qe["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() - t0 > timeout:
                self.athena.stop_query_execution(QueryExecutionId=qid)
                raise TimeoutError(f"Athena: '{label}' sigue {state} despues de {timeout}s")
            time.sleep(pause)
            pause = min(pause * 1.5, 3.0)
        if state != "SUCCEEDED":
            raise RuntimeError(f"Athena {state} en '{label}': {qe['Status'].get('StateChangeReason', '')}")
        return {"query_id": qid, "bytes_scanned": int(qe.get("Statistics", {}).get("DataScannedInBytes", 0)),
                "seconds": round(time.time() - t0, 1)}

    def rows(self, sql, label=""):
        info = self.execute(sql, label)
        header, out = None, []
        for page in self.athena.get_paginator("get_query_results").paginate(QueryExecutionId=info["query_id"]):
            for r in page["ResultSet"]["Rows"]:
                vals = [c.get("VarCharValue") for c in r["Data"]]
                if header is None:
                    header = vals
                else:
                    out.append(vals)
        return header or [], out

    # -- pasos del plan
    def ensure_sources(self):
        self.execute(f"CREATE DATABASE IF NOT EXISTS {self.cfg['database']}", "create database", use_database=False)
        for name, ddl in source_ddls(self.cfg).items():
            self.execute(f"DROP TABLE IF EXISTS {name}", f"drop {name}")
            self.execute(ddl, f"create {name}")
        return {}

    def ctas(self, table, select_sql, partitioned_by, run_id):
        loc = f"{self.cfg['star_uri']}{table}/v={run_id}/"
        props = ["format = 'PARQUET'", "write_compression = 'SNAPPY'", f"external_location = '{loc}'"]
        if partitioned_by:
            props.append("partitioned_by = ARRAY[" + ", ".join(f"'{p}'" for p in partitioned_by) + "]")
        self.execute(f"DROP TABLE IF EXISTS {table}", f"drop {table}")
        sql = f"CREATE TABLE {table}\nWITH ({', '.join(props)})\nAS\n{_fill(select_sql)}"
        info = self.execute(sql, f"ctas {table}")
        info["location"] = loc
        return info

    def checks(self, sql):
        header, rows = self.rows(sql, "checks")
        return {h: (int(v) if v not in (None, "") else None) for h, v in zip(header, rows[0])} if rows else {}

    def _delete_prefix(self, uri, keep_prefix=None):
        bucket, prefix = split_s3(uri)
        star_bucket, star_prefix = split_s3(self.cfg["star_uri"])
        raw_bucket, raw_prefix = split_s3(self.cfg["raw_uri"])
        allowed = (bucket == star_bucket and prefix.startswith(star_prefix)) or \
                  (bucket == raw_bucket and prefix.startswith(raw_prefix))
        if not allowed or not prefix:
            raise RuntimeError(f"Por seguridad solo se borra dentro de star/ o de la landing zone: {uri}")
        deleted, batch = 0, []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                if keep_prefix and o["Key"].startswith(keep_prefix):
                    continue
                batch.append({"Key": o["Key"]})
                if len(batch) == 1000:
                    self.s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
                    deleted, batch = deleted + len(batch), []
        if batch:
            self.s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
            deleted += len(batch)
        return deleted

    def cleanup(self, run_id):
        """Borra las versiones viejas de cada tabla (deja solo v=<run_id>)."""
        n = 0
        for table, _, _ in PLAN:
            uri = f"{self.cfg['star_uri']}{table}/"
            _, keep = split_s3(f"{uri}v={run_id}/")
            n += self._delete_prefix(uri, keep_prefix=keep)
        return {"old_objects_deleted": n}

    def reset(self, landing=False):
        for table, _, _ in PLAN:
            self.execute(f"DROP TABLE IF EXISTS {table}", f"drop {table}")
        out = {"star_objects_deleted": self._delete_prefix(self.cfg["star_uri"])}
        if landing:
            out["landing_objects_deleted"] = self._delete_prefix(self.cfg["raw_uri"])
        return out


# -------------------------------------------------------------------------------------- el job
def make_engine():
    if os.environ.get("STAR_ENGINE", "athena") == "duckdb":   # solo para la prueba local (tests_local/)
        from duck_engine import DuckEngine
        return DuckEngine.from_env()
    return AthenaEngine(load_config())


def run_refresh(engine=None, progress=None):
    """Una corrida completa del ETL. Devuelve un reporte (pasos, tiempos, bytes, conteos)."""
    engine = engine or make_engine()
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    t_start = time.time()
    report = {"run_id": run_id, "engine": engine.name, "steps": []}

    def step(name, fn):
        if progress:
            progress(name)
        t0 = time.time()
        out = fn() or {}
        report["steps"].append({"step": name, "seconds": round(time.time() - t0, 1),
                                **{k: v for k, v in out.items() if k in ("bytes_scanned", "query_id",
                                                                       "old_objects_deleted")}})
        return out

    step("sources", engine.ensure_sources)
    for table, sql, parts in PLAN:
        step(table, lambda t=table, s=sql, p=parts: engine.ctas(t, s, p, run_id))
    report["checks"] = step("checks", lambda: engine.checks(SQL_CHECKS))
    step("cleanup", lambda: engine.cleanup(run_id))
    report["seconds"] = round(time.time() - t_start, 1)
    report["bytes_scanned"] = sum(s.get("bytes_scanned", 0) for s in report["steps"])
    return report


# ----------------------------------------------------------------------------------------- setup
def setup(cfg, ref_dir):
    """Una sola vez: workgroup con limite por consulta (R10), base de datos y CSV de referencia en S3."""
    import boto3
    s = boto3.Session(region_name=cfg["region"])
    athena, s3 = s.client("athena"), s.client("s3")
    conf = {"ResultConfiguration": {"OutputLocation": cfg["athena_output"]},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": False,
            "BytesScannedCutoffPerQuery": SCAN_LIMIT_BYTES}
    try:
        athena.create_work_group(Name=cfg["workgroup"], Configuration=conf,
                                 Description="Parcial 3 - modelo estrella. Limite por consulta 1e9 bytes (R10)")
        print(f"workgroup {cfg['workgroup']} creado")
    except athena.exceptions.InvalidRequestException as e:
        if "already" not in str(e).lower() and "exist" not in str(e).lower():
            raise
        athena.update_work_group(WorkGroup=cfg["workgroup"], ConfigurationUpdates={
            "ResultConfigurationUpdates": {"OutputLocation": cfg["athena_output"]},
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": False,
            "BytesScannedCutoffPerQuery": SCAN_LIMIT_BYTES})
        print(f"workgroup {cfg['workgroup']} actualizado")
    wg = athena.get_work_group(WorkGroup=cfg["workgroup"])["WorkGroup"]["Configuration"]
    print("  limite por consulta:", wg.get("BytesScannedCutoffPerQuery"), "bytes")

    bucket, prefix = split_s3(cfg["ref_uri"])
    for local, folder in (("routes_ref.csv", "routes"), ("route_category_history.csv", "route_category_history"),
                          ("holidays_ref.csv", "holidays")):
        path = os.path.join(ref_dir, local)
        if not os.path.exists(path):
            raise FileNotFoundError(f"{path} no existe: corre primero python -m airline_data.generate_p3 "
                                    f"--seed <SEED> --out {ref_dir}")
        key = f"{prefix}{folder}/{local}"
        s3.upload_file(path, bucket, key)
        print(f"  subido s3://{bucket}/{key}")
    engine = AthenaEngine(cfg)
    engine.ensure_sources()
    print(f"base {cfg['database']} y tablas fuente listas")


# ------------------------------------------------------------------------------------------- CLI
def main(argv=None):
    ap = argparse.ArgumentParser(description="ETL del modelo estrella (Parcial 3)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("setup")
    p.add_argument("--ref-dir", default="ref")
    sub.add_parser("run")
    sub.add_parser("checks")
    p = sub.add_parser("reset")
    p.add_argument("--landing", action="store_true", help="tambien vacia la landing zone (RAW_EVENTS_URI)")
    p.add_argument("--yes", action="store_true")
    a = ap.parse_args(argv)

    if a.cmd == "setup":
        setup(load_config(), a.ref_dir)
    elif a.cmd == "run":
        rep = run_refresh(progress=lambda s: print(f"  -> {s}", flush=True))
        print(json.dumps(rep, indent=2))
    elif a.cmd == "checks":
        print(json.dumps(make_engine().checks(SQL_CHECKS), indent=2))
    elif a.cmd == "reset":
        what = "el star" + (" Y TODA LA LANDING ZONE (" + os.environ.get("RAW_EVENTS_URI", "?") + ")"
                            if a.landing else "")
        if not a.yes:
            sys.exit(f"Esto borra {what}. Repite con --yes si estas seguro.")
        print(json.dumps(make_engine().reset(landing=a.landing), indent=2))


if __name__ == "__main__":
    main()
