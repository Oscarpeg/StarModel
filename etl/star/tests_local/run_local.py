"""
run_local.py - Corre los tests OFICIALES de R5-R10 (tests/test_2_star.py) contra star_etl.py + star.py,
sin AWS: landing zone local, Athena simulado con DuckDB y el backend levantado con uvicorn --workers 2.

    python tests_local/run_local.py --harness /ruta/parcial3-harness-estudiantes --seed 417
    python tests_local/run_local.py --harness ... --seed 417 --salt 7 --keep-landing   # escenario sustentacion
"""
import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
import datetime as dt

HERE = os.path.dirname(os.path.abspath(__file__))
STAR_DIR = os.path.dirname(HERE)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def write_noise(landing, routes):
    """Trafico 'vivo' y basura que el pipeline debe aguantar sin romper R6 (huerfanos) ni R8."""
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    iso = lambda t: t.strftime("%Y-%m-%dT%H:%M:%SZ")
    r0 = routes[0]
    o, d = r0.split("-")
    ev = []
    for i in range(5):   # busquedas reales (no rp-) de hoy, una ruta que no existe y una con espacios
        ev.append({"event_id": f"live-s{i}", "event_type": "search_performed", "event_ts": iso(now),
                   "schema_version": 1, "session_id": f"sess-{i}", "origin": o, "destination": d,
                   "travel_date": "2031-03-10", "results_count": 2})
    ev.append({"event_id": "live-unknown-route", "event_type": "search_performed", "event_ts": iso(now),
               "schema_version": 1, "session_id": "x", "origin": "BOG", "destination": "XYZ",
               "travel_date": "2031-03-10", "results_count": 0})
    ev.append({"event_id": "live-c1", "event_type": "reservation_created", "event_ts": iso(now), "schema_version": 1,
               "reservation_id": "R-1", "flight_id": "F-1", "route": f" {o.lower()} - {d} ", "flight_date": "2031-03-10",
               "fare_class": "economy", "price_cents": 100000})
    ev.append({"event_id": "live-c2", "event_type": "reservation_created", "event_ts": iso(now), "schema_version": 1,
               "reservation_id": "R-2", "flight_id": "F-1", "route": r0, "flight_date": "2031-03-10",
               "fare_class": "PREMIUM", "price_cents": 100000})
    ev.append({"event_id": "live-x1", "event_type": "reservation_cancelled", "event_ts": iso(now), "schema_version": 1,
               "reservation_id": "R-1", "flight_id": "F-1", "route": r0, "flight_date": "2031-03-10",
               "fare_class": "ECONOMY", "price_cents": 100000})
    bad = [{"event_type": "search_performed", "event_ts": iso(now)},                                  # sin event_id
           {"event_id": "bad-type", "event_type": "page_view", "event_ts": iso(now)},                   # tipo desconocido
           {"event_id": "bad-ts", "event_type": "search_performed", "event_ts": "ayer a las 3"},        # fecha mala
           {"event_id": "bad-cal", "event_type": "search_performed", "event_ts": "2031-01-01T00:00:00Z",
            "origin": o, "destination": d},                                                           # fuera del calendario
           {"event_id": "bad-price", "event_type": "reservation_created", "event_ts": iso(now), "route": r0,
            "fare_class": "ECONOMY"}]                                                                # reserva sin precio
    folder = os.path.join(landing, "noise")
    os.makedirs(folder, exist_ok=True)
    lines = [json.dumps(e) for e in ev + ev[:3] + bad] + ["esto no es json", ""]   # + duplicados
    with open(os.path.join(folder, f"0000-noise-{uuid.uuid4()}.json"), "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(ev)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--harness", required=True)
    ap.add_argument("--seed", type=int, default=417)
    ap.add_argument("--salt", type=int, default=0)
    ap.add_argument("--sim-dir", default="/tmp/star_sim")
    ap.add_argument("--keep-landing", action="store_true", help="no vaciar la landing zone antes de correr")
    ap.add_argument("--no-noise", action="store_true")
    ap.add_argument("--workers", type=int, default=2)
    a = ap.parse_args()

    harness = os.path.abspath(a.harness)
    sim = os.path.abspath(a.sim_dir)
    landing, ref, state = (os.path.join(sim, x) for x in ("landing", "ref", "state"))
    if not a.keep_landing:
        shutil.rmtree(sim, ignore_errors=True)
    for p in (landing, ref, state):
        os.makedirs(p, exist_ok=True)

    sys.path.insert(0, harness)
    from airline_data.events import write_reference_files, routes_ref
    write_reference_files(a.seed, ref)
    if not a.no_noise and not a.keep_landing:
        write_noise(landing, list(routes_ref(a.seed).route))

    port = free_port()
    env = dict(os.environ,
               STAR_ENGINE="duckdb", STAR_DUCKDB_PATH=os.path.join(sim, "star.duckdb"),
               STAR_LOCAL_LANDING=landing, STAR_LOCAL_REF=ref, STAR_STATE_DIR=state,
               PYTHONPATH=os.pathsep.join([STAR_DIR, HERE, harness]),
               SIM_DIR=sim, SIM_SEED=str(a.seed), SIM_SALT=str(a.salt), SIM_HARNESS=harness,
               SIM_BASE_URL=f"http://127.0.0.1:{port}", HARNESS_REPORT=os.path.join(sim, "report"))
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app_local:app", "--app-dir", HERE,
                            "--host", "127.0.0.1", "--port", str(port), "--workers", str(a.workers),
                            "--log-level", "warning"], env=env, start_new_session=True)
    try:
        import httpx
        for _ in range(60):
            try:
                if httpx.get(f"http://127.0.0.1:{port}/health", timeout=1).status_code == 200:
                    break
            except Exception:
                time.sleep(0.5)
        t0 = time.time()
        rc = subprocess.call([sys.executable, "-m", "pytest", "tests/test_2_star.py", "-p", "localsim_plugin",
                              "-q", "-rfE", "--tb=line"], cwd=harness, env=env)
        print(f"\npytest exit {rc} en {time.time() - t0:.0f}s")
        st = httpx.get(f"http://127.0.0.1:{port}/admin/refresh-status", timeout=5).json()
        print("refresh-status:", json.dumps({k: st[k] for k in ("state", "job_id")}),
              "| checks:", json.dumps((st.get("last_run") or {}).get("checks")))
    finally:
        os.killpg(srv.pid, signal.SIGTERM)
        srv.wait(timeout=10)
    rep = json.load(open(os.path.join(sim, "report.json")))
    print(f"\nPUNTAJE R5-R10 (simulado): {sum(r['points'] for r in rep['requirements'] if r['id'] in ('R5','R6','R7','R8','R9','R10'))} / 41")
    for r in rep["requirements"]:
        if r["id"] in ("R5", "R6", "R7", "R8", "R9", "R10"):
            print(f"  {r['id']:4} {r['status']:12} {r['points']}/{r['weight']}")
            for t in r["tests"]:
                if t["outcome"] != "passed":
                    print(f"       - {t['outcome']}: {t['name']} {t['detail'][:220]}")


if __name__ == "__main__":
    main()
