"""
aws_direct_plugin.py - Autotest de R5-R10 en AWS REAL (Athena, Glue, S3) sin depender de Kinesis ni del
backend de la compañera. Lo usa run_aws_selftest.py desde CloudShell.

Lo unico que cambia frente al harness oficial: el replay, en vez de ir a Kinesis y esperar a que la
compañera lo lleve a S3, se escribe DIRECTO en una landing zone de prueba (SELFTEST_RAW_URI), con el
mismo formato que dejaria Firehose (JSON pegados, .gz). Todo lo demas (Athena, Glue, las consultas de
los tests y el oraculo) es el harness tal cual.
"""
import copy
import datetime as dt
import gzip
import os
import uuid

from harness import registry
from harness.aws import RealAWS, split_s3
from harness.config import load_config

_cfg = copy.deepcopy(load_config())
_cfg["aws"]["raw_events_uri"] = os.environ["SELFTEST_RAW_URI"]
_cfg["backend"]["base_url"] = os.environ["SELFTEST_BASE_URL"]
registry.CONFIG_OVERRIDE = _cfg


class DirectAWS(RealAWS):
    def kinesis_put(self, stream, records):
        bucket, prefix = split_s3(os.environ["SELFTEST_RAW_URI"])
        s3 = self._c("s3")
        now = dt.datetime.now(dt.timezone.utc)
        for n, i in enumerate(range(0, len(records), 5000)):
            body = gzip.compress(b"".join(d for _, d in records[i:i + 5000]))
            key = (f"{prefix}{now:%Y/%m/%d/%H}/selftest-{now:%Y-%m-%d-%H-%M-%S}-{n:03d}-"
                   f"{uuid.uuid4().hex[:8]}.gz")
            s3.put_object(Bucket=bucket, Key=key, Body=body)


registry.AWS_OVERRIDE = DirectAWS(_cfg["aws"].get("region", "us-east-1"), _cfg["aws"].get("athena_output"))
