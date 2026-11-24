"""
run_aws_selftest.py - Corre los tests oficiales de R5-R10 contra AWS real desde CloudShell, sin Kinesis.

  1. Vacia la landing zone DE PRUEBA (s3://<bucket>/selftest/raw/events/, nunca la real).
  2. Levanta star.py en localhost con uvicorn (motor Athena real, RAW_EVENTS_URI = la de prueba).
  3. Corre pytest tests/test_2_star.py del harness con aws_direct_plugin.

Requisitos: config.yaml del harness ya lleno (seed, athena_workgroup, athena_database, athena_output) y
las variables STAR_BUCKET / STAR_WORKGROUP / STAR_DATABASE exportadas.

    python3 tests_local/run_aws_selftest.py --harness ~/parcial3-harness-estudiantes
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
STAR_DIR = os.path.dirname(HERE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--harness", required=True)
    ap.add_argument("--port", type=int, default=8765)
    a = ap.parse_args()
    harness = os.path.abspath(os.path.expanduser(a.harness))
    bucket = os.environ["STAR_BUCKET"]
    raw = f"s3://{bucket}/selftest/raw/events/"

    import boto3
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="selftest/raw/events/"):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
            n += len(objs)
    print(f"landing zone de prueba vaciada ({n} objetos): {raw}")

    env = dict(os.environ, RAW_EVENTS_URI=raw, STAR_ENGINE="athena",
               STAR_STATE_DIR=os.path.join(HERE, ".state_selftest"),
               PYTHONPATH=os.pathsep.join([STAR_DIR, HERE, harness]),
               SELFTEST_RAW_URI=raw, SELFTEST_BASE_URL=f"http://127.0.0.1:{a.port}",
               HARNESS_REPORT=os.path.join(HERE, "selftest_report"))
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app_local:app", "--app-dir", HERE,
                            "--host", "127.0.0.1", "--port", str(a.port), "--log-level", "warning"],
                           env=env, start_new_session=True)
    try:
        import httpx
        for _ in range(60):
            try:
                if httpx.get(f"http://127.0.0.1:{a.port}/health", timeout=1).status_code == 200:
                    break
            except Exception:
                time.sleep(0.5)
        t0 = time.time()
        subprocess.call([sys.executable, "-m", "pytest", "tests/test_2_star.py", "-p", "aws_direct_plugin",
                         "-q", "-rfE", "--tb=line"], cwd=harness, env=env)
        print(f"\nduracion: {time.time() - t0:.0f}s")
        st = httpx.get(f"http://127.0.0.1:{a.port}/admin/refresh-status", timeout=10).json()
        print(json.dumps(st, indent=2, default=str)[:3000])
    finally:
        os.killpg(srv.pid, signal.SIGTERM)
        srv.wait(timeout=10)
    rep = json.load(open(os.path.join(HERE, "selftest_report.json")))
    pts = sum(r["points"] for r in rep["requirements"] if r["id"] in ("R5", "R6", "R7", "R8", "R9", "R10"))
    print(f"\nPUNTAJE R5-R10 en AWS (sin Kinesis): {pts} / 41")
    for r in rep["requirements"]:
        if r["id"] in ("R5", "R6", "R7", "R8", "R9", "R10"):
            print(f"  {r['id']:4} {r['status']:12} {r['points']}/{r['weight']}")
            for t in r["tests"]:
                if t["outcome"] != "passed":
                    print(f"       - {t['name']}: {t['detail'][:250]}")
    print("\nOJO: esto dejo el star construido con los datos de prueba. Al final, el refresh real "
          "(POST /admin/refresh-star en la EC2) lo reconstruye desde la landing zone real.")


if __name__ == "__main__":
    main()
