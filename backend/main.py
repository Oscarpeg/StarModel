import os
import uuid
from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.errors import UniqueViolation
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field
from analytics import router as analytics_router
import events  # Parcial 3: Kinesis, vista en vivo y zona de aterrizaje

DB_URL = (
    f"host={os.environ.get('DB_HOST', 'localhost')} "
    f"port={os.environ.get('DB_PORT', '5432')} "
    f"dbname={os.environ.get('DB_NAME', 'airline')} "
    f"user={os.environ.get('DB_USER', 'postgres')} "
    f"password={os.environ.get('DB_PASSWORD', '')}"
)

# Pool de conexiones: se abren una vez y se reutilizan. Si llegan mas peticiones que
# conexiones, esperan en cola (hasta 30 s) en vez de fallar con 500.
pool = ConnectionPool(DB_URL, min_size=2, max_size=15, timeout=30, open=False)

STATIC = Path(__file__).parent / "static"
app = FastAPI(title="Airline reservations")
app.include_router(analytics_router)

# Parcial 3: el router del modelo estrella (POST /admin/refresh-star, GET /admin/refresh-status)
# lo entrega el integrante B en star.py. Mientras no exista, el backend arranca igual.
try:
    from star import router as star_router
    app.include_router(star_router)
except ImportError:
    pass


@app.on_event("startup")
def startup():
    pool.open(wait=True)
    events.start_background_threads()   # hilo publicador + hilo consumidor del stream


@app.on_event("shutdown")
def shutdown():
    pool.close()


class FlightIn(BaseModel):
    flight_id: str = Field(min_length=1)
    route: str = Field(pattern=r"^[A-Z]{3}-[A-Z]{3}$")
    flight_date: date
    capacity: int = Field(gt=0)
    price_cents: int = Field(ge=0)


class ReservationIn(BaseModel):
    flight_id: str
    passenger_name: str = Field(min_length=1)
    fare_class: Literal["ECONOMY", "BUSINESS"]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/flights", status_code=201)
def create_flight(f: FlightIn):
    try:
        with pool.connection() as conn:
            conn.execute(
                "INSERT INTO flights (flight_id, route, flight_date, capacity, seats_available, price_cents) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (f.flight_id, f.route, f.flight_date, f.capacity, f.capacity, f.price_cents),
            )
    except UniqueViolation:
        raise HTTPException(409, "flight_id already exists")
    return {**f.model_dump(), "flight_date": f.flight_date.isoformat()}


@app.get("/flights/search")
def search_flights(
    origin: str = Query(pattern=r"^[A-Za-z]{3}$"),
    destination: str = Query(pattern=r"^[A-Za-z]{3}$"),
    flight_date: date = Query(alias="date"),
    x_session_id: str | None = Header(default=None),
):
    route = f"{origin.upper()}-{destination.upper()}"
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT flight_id, route, flight_date, seats_available, price_cents FROM flights "
            "WHERE route = %s AND flight_date = %s AND seats_available > 0 ORDER BY flight_id",
            (route, flight_date),
        ).fetchall()
    # Parcial 3: un evento por busqueda. Se deja en la cola y se responde sin esperar a Kinesis.
    events.publish_async(events.make_event(
        "search_performed", session_id=x_session_id or str(uuid.uuid4()),
        origin=origin.upper(), destination=destination.upper(),
        travel_date=flight_date.isoformat(), results_count=len(rows)))
    return [
        {"flight_id": r[0], "route": r[1], "flight_date": r[2].isoformat(),
         "seats_available": r[3], "price_cents": r[4]}
        for r in rows
    ]


@app.post("/reservations", status_code=201)
def create_reservation(r: ReservationIn):
    reservation_id = str(uuid.uuid4())
    with pool.connection() as conn:
        # Todo dentro de UNA transaccion: si algo falla, no queda un cupo descontado sin reserva.
        with conn.transaction():
            # UPDATE atomico: resta un cupo SOLO si todavia queda alguno. PostgreSQL bloquea la fila,
            # asi que dos peticiones simultaneas nunca se quedan con el mismo cupo.
            taken = conn.execute(
                "UPDATE flights SET seats_available = seats_available - 1 "
                "WHERE flight_id = %s AND seats_available > 0 RETURNING flight_id, route, flight_date, price_cents",
                (r.flight_id,),
            ).fetchone()
            if taken is None:
                exists = conn.execute("SELECT 1 FROM flights WHERE flight_id = %s", (r.flight_id,)).fetchone()
                if exists is None:
                    raise HTTPException(404, "flight not found")
                raise HTTPException(409, "no seats left")
            conn.execute(
                "INSERT INTO reservations (reservation_id, flight_id, passenger_name, fare_class, status) "
                "VALUES (%s, %s, %s, %s, 'CONFIRMED')",
                (reservation_id, r.flight_id, r.passenger_name, r.fare_class),
            )
    # Parcial 3: se publica DESPUES del commit (la reserva ya existe). Sin passenger_name.
    events.publish_sync(events.make_event(
        "reservation_created", reservation_id=reservation_id, flight_id=r.flight_id, route=taken[1],
        flight_date=taken[2].isoformat(), fare_class=r.fare_class, price_cents=int(taken[3])))
    return {"reservation_id": reservation_id, "flight_id": r.flight_id,
            "status": "CONFIRMED", "passenger_name": r.passenger_name}


@app.post("/reservations/{reservation_id}/cancel")
def cancel_reservation(reservation_id: str):
    """Parcial 3: marca la reserva como CANCELLED, libera el asiento y publica el evento."""
    with pool.connection() as conn:
        with conn.transaction():
            # FOR UPDATE bloquea la reserva: dos cancelaciones al tiempo no liberan dos asientos.
            row = conn.execute(
                "SELECT r.reservation_id, r.flight_id, r.status, r.passenger_name, r.fare_class, "
                "f.route, f.flight_date, f.price_cents "
                "FROM reservations r JOIN flights f ON f.flight_id = r.flight_id "
                "WHERE r.reservation_id = %s FOR UPDATE OF r",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(404, "reservation not found")
            if row[2] == "CANCELLED":
                raise HTTPException(409, "reservation already cancelled")
            conn.execute("UPDATE reservations SET status = 'CANCELLED' WHERE reservation_id = %s",
                         (reservation_id,))
            conn.execute("UPDATE flights SET seats_available = seats_available + 1 WHERE flight_id = %s",
                         (row[1],))
    events.publish_sync(events.make_event(
        "reservation_cancelled", reservation_id=row[0], flight_id=row[1], route=row[5],
        flight_date=row[6].isoformat(), fare_class=row[4], price_cents=int(row[7])))
    return {"reservation_id": row[0], "flight_id": row[1], "status": "CANCELLED", "passenger_name": row[3]}


@app.get("/analytics/live")
def analytics_live(window: int = Query(default=5, ge=1, le=60)):
    """Parcial 3: conteos de los ultimos `window` minutos segun event_ts. Sale de los contadores
    en memoria que alimenta el consumidor del stream (no de Athena)."""
    return events.live_counts(window)


@app.get("/admin/events-status", include_in_schema=False)
def events_status():
    """Para revisar que el consumidor este leyendo y subiendo a S3 (no lo usa el harness)."""
    return events.status()


@app.get("/reservations/{reservation_id}")
def get_reservation(reservation_id: str):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT reservation_id, flight_id, status, passenger_name FROM reservations WHERE reservation_id = %s",
            (reservation_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(404, "reservation not found")
    return {"reservation_id": row[0], "flight_id": row[1], "status": row[2], "passenger_name": row[3]}


@app.get("/", include_in_schema=False)
def page_customer():
    return FileResponse(STATIC / "index.html")


@app.get("/manager", include_in_schema=False)
def page_manager():
    return FileResponse(STATIC / "manager.html")


# ---------- catalogo para que el frontend muestre opciones (no lo usa el harness) ----------
import time as _time

import boto3 as _boto3


@app.get("/flights/routes")
def list_routes():
    """Rutas que tienen al menos un vuelo con cupo (para las listas de origen y destino)."""
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT route FROM flights WHERE seats_available > 0 ORDER BY route"
        ).fetchall()
    return [{"route": r[0], "origin": r[0].split("-")[0], "destination": r[0].split("-")[1]} for r in rows]


@app.get("/flights/dates")
def list_dates(origin: str = Query(pattern=r"^[A-Za-z]{3}$"),
               destination: str = Query(pattern=r"^[A-Za-z]{3}$")):
    """Fechas con vuelos disponibles para una ruta (para los botones de fechas)."""
    route = f"{origin.upper()}-{destination.upper()}"
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT flight_date, count(*), sum(seats_available) FROM flights "
            "WHERE route = %s AND seats_available > 0 GROUP BY flight_date ORDER BY flight_date LIMIT 60",
            (route,),
        ).fetchall()
    return [{"date": r[0].isoformat(), "flights": r[1], "seats": int(r[2])} for r in rows]


_CATALOG = {"data": None, "at": 0.0}


@app.get("/analytics/catalog")
def analytics_catalog():
    """Rutas y meses que existen en route_month_metrics (Athena), para el panel del gerente.
    Se guarda en memoria 1 hora: solo cambia cuando corre el job de EMR."""
    if _CATALOG["data"] is not None and _time.time() - _CATALOG["at"] < 3600:
        return _CATALOG["data"]
    athena = _boto3.client("athena", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    kw = {"QueryString": "SELECT DISTINCT route, month FROM route_month_metrics",
          "QueryExecutionContext": {"Database": os.environ.get("ATHENA_DATABASE", "airline_analytics")},
          "WorkGroup": os.environ.get("ATHENA_WORKGROUP", "primary"),
          "ResultConfiguration": {"OutputLocation": os.environ.get(
              "ATHENA_OUTPUT", "s3://parcial2-aerolinea-838910/athena-results/")}}
    qid = athena.start_query_execution(**kw)["QueryExecutionId"]
    deadline = _time.time() + 15
    while True:
        st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st == "SUCCEEDED":
            break
        if st in ("FAILED", "CANCELLED") or _time.time() > deadline:
            raise HTTPException(503, "catalog not available")
        _time.sleep(0.3)
    rows = []
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in page["ResultSet"]["Rows"]]
    rows = rows[1:]  # la primera fila son los encabezados
    data = {"routes": sorted({r[0] for r in rows}), "months": sorted({r[1] for r in rows})}
    _CATALOG.update(data=data, at=_time.time())
    return data
