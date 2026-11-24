"""Parcial 3 - Eventos en Kinesis, vista en vivo y zona de aterrizaje en S3.

Tres piezas que corren dentro del mismo backend (monolito):

1. Publicador asincrono (busquedas): la peticion solo deja el evento en una cola en memoria y
   responde de una vez. Un hilo aparte manda la cola a Kinesis en lotes (PutRecords) y reintenta
   los registros que Kinesis rechaza. Asi publicar no hace lenta la busqueda (R2).
2. Publicador sincrono (reservas y cancelaciones): se llama DESPUES de guardar en la base y
   reintenta varias veces, porque son transacciones del negocio y no se deben perder (R1).
3. Consumidor del stream: UN hilo que lee el shard con GetRecords y hace dos cosas con cada evento:
   a) suma el evento en contadores por segundo segun su event_ts -> GET /analytics/live (R3)
   b) lo guarda en un buffer que se sube a S3 cada minuto como JSON lines gzip (R4)
   Guarda un checkpoint (ultimo numero de secuencia subido a S3) en un archivo local para que,
   si el servicio se reinicia, siga desde ahi y no pierda eventos.

Importante: uvicorn debe correr con --workers 1. Con varios procesos habria varios consumidores
leyendo el mismo shard, cada uno con sus propios contadores.
No hay credenciales en el codigo: boto3 usa el LabInstanceProfile de la EC2.
"""
import gzip
import json
import logging
import os
import queue
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.config import Config

log = logging.getLogger("events")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

REGION = os.environ.get("AWS_REGION", "us-east-1")
STREAM = os.environ.get("KINESIS_STREAM", "airline-events")
RAW_URI = os.environ.get("RAW_EVENTS_URI", "s3://parcial2-aerolinea-838910/raw/events/")
CHECKPOINT_FILE = os.environ.get("CHECKPOINT_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                 ".kinesis_checkpoint.json"))
FLUSH_SECONDS = float(os.environ.get("LANDING_FLUSH_SECONDS", "60"))   # cada cuanto se sube a S3
FLUSH_MAX_EVENTS = int(os.environ.get("LANDING_FLUSH_MAX_EVENTS", "20000"))
LIVE_KEEP_SECONDS = 3600          # la vista en vivo guarda la ultima hora (window maximo 60 min)
SCHEMA_VERSION = 1

_cfg = Config(region_name=REGION, connect_timeout=2, read_timeout=5,
              retries={"max_attempts": 3, "mode": "standard"})
_kinesis = boto3.client("kinesis", config=_cfg)
_s3 = boto3.client("s3", config=_cfg)


# --------------------------------------------------------------------------------- utilidades
def now_ts() -> str:
    """Hora actual en UTC con el formato del contrato: YYYY-MM-DDTHH:MM:SSZ."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_event(event_type: str, **fields) -> dict:
    """Sobre comun del contrato + los campos propios de cada tipo. Nunca lleva datos personales."""
    return {"event_id": str(uuid.uuid4()), "event_type": event_type, "event_ts": now_ts(),
            "schema_version": SCHEMA_VERSION, **fields}


def partition_key(event: dict) -> str:
    """Busquedas por session_id; reservas y cancelaciones por flight_id (contrato A.1)."""
    if event["event_type"] == "search_performed":
        return event["session_id"]
    return event["flight_id"]


def _encode(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


# ------------------------------------------------------------- 1. publicador asincrono (busquedas)
_queue: "queue.Queue[dict]" = queue.Queue(maxsize=100_000)


def publish_async(event: dict) -> None:
    """No bloquea nunca. Si la cola estuviera llena (Kinesis caido mucho tiempo) se descarta la
    busqueda: perder una busqueda es aceptable, hacer lento al usuario no."""
    try:
        _queue.put_nowait(event)
    except queue.Full:
        log.warning("cola llena: se descarta la busqueda %s", event["event_id"])


def _put_records_with_retry(events: list, attempts: int = 8) -> list:
    """Manda hasta 500 eventos por llamada y reintenta solo los que Kinesis rechazo (throttling).
    Devuelve los que no se pudieron mandar despues de todos los intentos."""
    pending = list(events)
    for attempt in range(attempts):
        failed = []
        for i in range(0, len(pending), 500):
            chunk = pending[i:i + 500]
            try:
                r = _kinesis.put_records(StreamName=STREAM, Records=[
                    {"Data": _encode(e), "PartitionKey": partition_key(e)} for e in chunk])
                failed += [chunk[j] for j, x in enumerate(r["Records"]) if "ErrorCode" in x]
            except Exception as ex:  # stream borrado, red, permisos...
                log.warning("put_records fallo (%s): %s", type(ex).__name__, ex)
                failed += chunk
        if not failed:
            return []
        pending = failed
        time.sleep(min(0.2 * 2 ** attempt, 5))
    return pending


def _publisher_loop():
    while True:
        batch = [_queue.get()]                     # espera el primer evento
        deadline = time.monotonic() + 0.1          # junta lo que llegue en 100 ms (lotes)
        while len(batch) < 500 and time.monotonic() < deadline:
            try:
                batch.append(_queue.get(timeout=max(0.0, deadline - time.monotonic())))
            except queue.Empty:
                break
        lost = _put_records_with_retry(batch)
        if lost:
            log.error("se perdieron %d busquedas despues de reintentar", len(lost))


# ------------------------------------------------------ 2. publicador sincrono (reservas)
def publish_sync(event: dict, attempts: int = 5) -> bool:
    """Para reservas y cancelaciones: se llama despues del COMMIT y reintenta. Si aun asi falla,
    la reserva ya quedo guardada y se registra el error (en produccion se usaria un outbox)."""
    for attempt in range(attempts):
        try:
            _kinesis.put_record(StreamName=STREAM, Data=_encode(event), PartitionKey=partition_key(event))
            return True
        except _kinesis.exceptions.ResourceNotFoundException:
            # el stream no existe (se borra al terminar de trabajar): no tiene sentido reintentar
            # y no se debe hacer esperar al usuario
            log.error("stream %s no existe: %s %s sin publicar", STREAM, event["event_type"], event["event_id"])
            return False
        except Exception as ex:
            log.warning("put_record intento %d fallo (%s): %s", attempt + 1, type(ex).__name__, ex)
            time.sleep(min(0.1 * 2 ** attempt, 2))
    log.error("NO se pudo publicar %s %s", event["event_type"], event["event_id"])
    return False


# -------------------------------------------------- 3. consumidor: vista en vivo + aterrizaje en S3
TYPE_KEY = {"search_performed": "searches", "reservation_created": "reservations",
            "reservation_cancelled": "cancellations"}

_live_lock = threading.Lock()
_live = defaultdict(lambda: {"searches": 0, "reservations": 0, "cancellations": 0})  # segundo -> conteos
_seen = {}  # event_id -> segundo: un evento repetido (reintento) se cuenta una sola vez
_status = {"consumer": "starting", "last_read": None, "events_read": 0, "landed": 0, "last_flush": None}


def _event_epoch(e: dict):
    try:
        return int(datetime.strptime(e["event_ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return None


def _count_live(e: dict) -> None:
    """Cuenta por event_ts (cuando paso), no por cuando llego. Un evento viejo (de la reproduccion
    del harness, o uno muy tardio) cae fuera de la ventana y no se cuenta."""
    key = TYPE_KEY.get(e.get("event_type"))
    sec = _event_epoch(e)
    if key is None or sec is None:
        return
    now = int(time.time())
    if sec < now - LIVE_KEEP_SECONDS or sec > now + 300:
        return
    with _live_lock:
        eid = e.get("event_id")
        if eid in _seen:
            return
        _seen[eid] = sec
        _live[sec][key] += 1


def live_counts(window_minutes: int) -> dict:
    now = int(time.time())
    start = now - window_minutes * 60
    tot = {"searches": 0, "reservations": 0, "cancellations": 0}
    with _live_lock:
        for sec in [s for s in _live if s < now - LIVE_KEEP_SECONDS]:   # limpia lo de hace mas de 1 h
            del _live[sec]
        for eid in [k for k, s in _seen.items() if s < now - LIVE_KEEP_SECONDS]:
            del _seen[eid]
        for sec, c in _live.items():
            if start < sec <= now + 300:
                for k in tot:
                    tot[k] += c[k]
    conv = tot["reservations"] / tot["searches"] if tot["searches"] else 0.0
    return {"window_minutes": window_minutes, **tot, "conversion": conv}


def _split_uri(uri: str):
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")
    return bucket, prefix


def _load_checkpoint(stream_id: str) -> dict:
    """{shard_id: ultimo_sequence_number_subido_a_S3}, solo si es del MISMO stream (si el stream se
    borra y se vuelve a crear, el checkpoint viejo no sirve)."""
    try:
        with open(CHECKPOINT_FILE) as fh:
            cp = json.load(fh)
        return cp.get("shards", {}) if cp.get("stream_id") == stream_id else {}
    except Exception:
        return {}


def _save_checkpoint(stream_id: str, shards: dict) -> None:
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({"stream_id": stream_id, "shards": shards}, fh)
    os.replace(tmp, CHECKPOINT_FILE)


class _Landing:
    """Buffer de eventos crudos que se sube a S3 como JSON lines gzip. La zona de aterrizaje es
    solo de agregar (append-only): si hay reintentos pueden quedar duplicados, el ETL los quita."""

    def __init__(self):
        self.bucket, self.prefix = _split_uri(RAW_URI)
        self.lines, self.pending_cp, self.t0 = [], {}, time.monotonic()

    def add(self, raw: bytes, shard_id: str, seq: str):
        self.lines.append(raw.decode("utf-8", errors="replace").strip())
        self.pending_cp[shard_id] = seq

    def due(self) -> bool:
        return bool(self.lines) and (len(self.lines) >= FLUSH_MAX_EVENTS
                                     or time.monotonic() - self.t0 >= FLUSH_SECONDS)

    def flush(self) -> dict:
        """Sube el lote y devuelve los checkpoints que ya quedaron seguros en S3."""
        if not self.lines:
            self.t0 = time.monotonic()
            return {}
        body = gzip.compress(("\n".join(self.lines) + "\n").encode("utf-8"))
        key = f"{self.prefix}events-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}.json.gz"
        for attempt in range(6):
            try:
                _s3.put_object(Bucket=self.bucket, Key=key, Body=body,
                               ContentType="application/json", ContentEncoding="gzip")
                break
            except Exception as ex:
                log.warning("subida a S3 fallo (%s), reintento", ex)
                time.sleep(min(2 ** attempt, 20))
        else:
            return {}   # no se subio: se conserva el buffer y se intenta en el siguiente ciclo
        n, cp = len(self.lines), self.pending_cp
        self.lines, self.pending_cp, self.t0 = [], {}, time.monotonic()
        _status["landed"] += n
        _status["last_flush"] = now_ts()
        log.info("subidos %d eventos a s3://%s/%s", n, self.bucket, key)
        return cp


def _consumer_loop():
    landing = _Landing()
    stream_id, checkpoint = None, {}
    while True:
        try:
            summ = _kinesis.describe_stream_summary(StreamName=STREAM)["StreamDescriptionSummary"]
            stream_id = f"{summ['StreamARN']}@{summ['StreamCreationTimestamp'].timestamp()}"
            if summ["StreamStatus"] not in ("ACTIVE", "UPDATING"):
                raise RuntimeError(f"stream {summ['StreamStatus']}")
            checkpoint = _load_checkpoint(stream_id)
            iters = {}
            for sh in _kinesis.list_shards(StreamName=STREAM)["Shards"]:
                sid = sh["ShardId"]
                if sid in checkpoint:
                    it = _kinesis.get_shard_iterator(StreamName=STREAM, ShardId=sid,
                                                     ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                                                     StartingSequenceNumber=checkpoint[sid])["ShardIterator"]
                else:  # stream nuevo: desde el principio, para no perder nada
                    it = _kinesis.get_shard_iterator(StreamName=STREAM, ShardId=sid,
                                                     ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
                iters[sid] = it
            _status["consumer"] = "running"
            log.info("consumidor leyendo %s (%d shards)", STREAM, len(iters))
            while iters:
                got_any = False
                for sid in list(iters):
                    try:
                        r = _kinesis.get_records(ShardIterator=iters[sid], Limit=10000)
                    except _kinesis.exceptions.ProvisionedThroughputExceededException:
                        time.sleep(1)
                        continue
                    for rec in r["Records"]:
                        got_any = True
                        try:
                            _count_live(json.loads(rec["Data"]))
                        except Exception:
                            pass
                        landing.add(rec["Data"], sid, rec["SequenceNumber"])
                        _status["events_read"] += 1
                    _status["last_read"] = now_ts()
                    if r.get("NextShardIterator"):
                        iters[sid] = r["NextShardIterator"]
                    else:          # shard cerrado (reshard): se deja de leer
                        del iters[sid]
                if landing.due():
                    done = landing.flush()
                    if done:
                        checkpoint.update(done)
                        _save_checkpoint(stream_id, checkpoint)
                # 1 shard admite 5 lecturas por segundo: 0.25 s entre vueltas deja margen
                time.sleep(0.05 if got_any else 0.25)
        except Exception as ex:
            # stream borrado o aun no creado, iterador vencido, red... se espera y se vuelve a empezar
            # desde el ultimo checkpoint. Lo que estaba en el buffer se sube primero.
            _status["consumer"] = f"waiting: {type(ex).__name__}"
            if landing.lines:
                done = landing.flush()
                if done and stream_id:
                    checkpoint.update(done)
                    _save_checkpoint(stream_id, checkpoint)
            log.warning("consumidor: %s: %s (reintento en 10 s)", type(ex).__name__, ex)
            time.sleep(10)


_started = False


def start_background_threads() -> None:
    global _started
    if _started or os.environ.get("EVENTS_DISABLED") == "1":
        return
    _started = True
    threading.Thread(target=_publisher_loop, name="kinesis-publisher", daemon=True).start()
    if os.environ.get("CONSUMER_DISABLED") != "1":
        threading.Thread(target=_consumer_loop, name="kinesis-consumer", daemon=True).start()


def status() -> dict:
    return {"stream": STREAM, "landing_zone": RAW_URI, "queue": _queue.qsize(), **_status}
