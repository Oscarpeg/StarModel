"""
star.py - Endpoints del refresh del modelo estrella (Parcial 3, Persona B).

    POST /admin/refresh-star    -> 202 {"job_id": "star-7", "state": "RUNNING"}
    GET  /admin/refresh-status  -> 200 {"state": "RUNNING" | "SUCCEEDED" | "FAILED", ...}

Integracion en main.py (igual que analytics.py en el parcial 2):

    from star import router as star_router
    app.include_router(star_router)

Por que el estado vive en un archivo y no en una variable:
uvicorn corre con --workers 2, o sea dos procesos. El POST puede caer en uno y el GET en el otro;
con el estado en memoria el GET responderia algo viejo y el harness creeria que ya termino.

Como funciona (un "sistema de turnos"):
  * Cada POST saca un turno: requested = requested + 1, y lanza un hilo con el job.
  * Los jobs corren de a uno: el hilo espera un lock de archivo (job.lock) antes de arrancar.
  * Al arrancar, el job atiende el turno mas alto pedido hasta ese momento; al terminar escribe
    completed = ese turno. Si llega un POST mientras otro job corre, ese POST tendra su propia corrida
    despues, que ya vera todos los eventos que estaban en S3 en el momento del POST.
  * El estado es SUCCEEDED/FAILED solo cuando completed >= requested; si no, es RUNNING.
"""
import datetime as dt
import fcntl
import json
import os
import threading
import time
import traceback
from contextlib import contextmanager

from fastapi import APIRouter

import star_etl

router = APIRouter(tags=["star"])

STATE_DIR = os.environ.get("STAR_STATE_DIR",
                           os.path.join(os.path.dirname(os.path.abspath(__file__)), ".star_state"))
STALE_SECONDS = int(os.environ.get("STAR_STALE_SECONDS", "120"))
_STATE = os.path.join(STATE_DIR, "state.json")
_META_LOCK = os.path.join(STATE_DIR, "meta.lock")   # protege leer/escribir state.json
_JOB_LOCK = os.path.join(STATE_DIR, "job.lock")     # garantiza un solo job a la vez
_DEFAULT = {"requested": 0, "completed": 0, "requested_at": None, "last_result": None,
            "last_error": None, "current": None, "last_run": None}


def _now():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def _lock(path, blocking=True):
    os.makedirs(STATE_DIR, exist_ok=True)
    f = open(path, "a+")
    got = False
    try:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            got = True
        except BlockingIOError:
            got = False
        yield got
    finally:
        if got:
            fcntl.flock(f, fcntl.LOCK_UN)
        f.close()


def _read():
    try:
        with open(_STATE, encoding="utf-8") as fh:
            s = json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        s = {}
    return {**_DEFAULT, **s}


def _write(s):
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp = f"{_STATE}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(s, fh, indent=2, default=str)
    os.replace(tmp, _STATE)   # reemplazo atomico: nadie lee un archivo a medio escribir


def _worker():
    with _lock(_JOB_LOCK):                       # espera su turno si hay otro job corriendo
        with _lock(_META_LOCK):
            s = _read()
            target = s["requested"]
            if s["completed"] >= target:         # una corrida anterior ya atendio este turno
                return
            s["current"] = {"ticket": target, "started_at": _now(), "step": "starting",
                            "heartbeat": time.time(), "pid": os.getpid()}
            _write(s)

        def progress(step):
            with _lock(_META_LOCK):
                st = _read()
                if st.get("current"):
                    st["current"].update(step=step, heartbeat=time.time())
                    _write(st)

        result, error, report = "SUCCEEDED", None, None
        try:
            report = star_etl.run_refresh(progress=progress)
        except Exception as exc:                 # el error queda visible en refresh-status
            result, error = "FAILED", f"{type(exc).__name__}: {exc}"
            traceback.print_exc()

        with _lock(_META_LOCK):
            s = _read()
            cur = s.get("current") or {}
            s["completed"] = max(s["completed"], target)
            s["last_result"], s["last_error"] = result, error
            s["last_run"] = {"ticket": target, "started_at": cur.get("started_at"), "finished_at": _now(),
                             "result": result, "error": error, "report": report}
            s["current"] = None
            _write(s)


@router.post("/admin/refresh-star", status_code=202)
def refresh_star():
    with _lock(_META_LOCK):
        s = _read()
        s["requested"] += 1
        s["requested_at"] = time.time()
        ticket = s["requested"]
        _write(s)
    threading.Thread(target=_worker, name=f"star-refresh-{ticket}", daemon=True).start()
    return {"job_id": f"star-{ticket}", "state": "RUNNING"}


@router.get("/admin/refresh-status")
def refresh_status():
    s = _read()
    detail = None
    if s["requested"] == 0:
        state, detail = "SUCCEEDED", "todavia no se ha pedido ningun refresh"
    elif s["completed"] >= s["requested"]:
        state = s["last_result"] or "SUCCEEDED"
    else:
        state = "RUNNING"
        # Si nadie tiene el lock del job y no hay actividad reciente, el proceso que lo corria murio
        # (reinicio de uvicorn o de la EC2): lo reportamos como FAILED para no quedar RUNNING para siempre.
        with _lock(_JOB_LOCK, blocking=False) as free:
            if free:
                last = max((s.get("current") or {}).get("heartbeat") or 0, s.get("requested_at") or 0)
                if time.time() - last > STALE_SECONDS:
                    state, detail = "FAILED", "el job se interrumpio (reinicio del servidor); haga POST de nuevo"
    last = s.get("last_run") or {}
    rep = last.get("report") or {}
    return {
        "state": state,
        "job_id": f"star-{s['requested']}" if s["requested"] else None,
        "detail": detail,
        "current_step": (s.get("current") or {}).get("step"),
        "last_run": {
            "job_id": f"star-{last['ticket']}" if last.get("ticket") else None,
            "result": last.get("result"),
            "started_at": last.get("started_at"),
            "finished_at": last.get("finished_at"),
            "seconds": rep.get("seconds"),
            "bytes_scanned": rep.get("bytes_scanned"),
            "error": last.get("error"),
            "checks": rep.get("checks"),
            "steps": rep.get("steps"),
        } if last else None,
    }
