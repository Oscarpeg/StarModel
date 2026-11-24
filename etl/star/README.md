# Modelo estrella - Parcial 3 (Persona B: R5-R10)

Landing zone (JSON crudo en S3) -> `stg_events` -> `dim_date`, `dim_route` (SCD 2), `dim_fare_class`,
`fact_search` (particionada por mes), `fact_booking_event`. Todo con Athena CTAS, disparado por
`POST /admin/refresh-star`.

| Archivo | Qué es |
|---|---|
| `star_etl.py` | El job de carga (ETL). CLI: `setup`, `run`, `checks`, `reset`. |
| `star.py` | Router FastAPI: `POST /admin/refresh-star` y `GET /admin/refresh-status`. Se monta en `main.py`. |
| `queries_q1_q8.sql` | Preguntas de negocio Q1-Q8 + consultas de evidencia (duplicados, poda, SCD 2). |
| `tests_local/run_local.py` | Corre los tests oficiales de R5-R10 sin AWS (DuckDB + landing zone local). |
| `tests_local/run_aws_selftest.py` | Corre los tests oficiales de R5-R10 en AWS real desde CloudShell, sin Kinesis. |

## Variables de entorno

```
STAR_BUCKET=parcial2-aerolinea-838910
RAW_EVENTS_URI=s3://parcial2-aerolinea-838910/raw/events/     # la misma raw_events_uri de config.yaml
STAR_WORKGROUP=airline-star                                    # la misma athena_workgroup de config.yaml
STAR_DATABASE=airline_star                                     # la misma athena_database de config.yaml
```

## Uso

```
python -m airline_data.generate_p3 --seed 417 --out ref/      # (desde la carpeta del harness)
python star_etl.py setup --ref-dir ref/                        # workgroup con límite 1e9 bytes, base, CSV
python star_etl.py run                                         # una corrida (lo mismo que el endpoint)
python star_etl.py reset --landing --yes                       # antes de la sustentación
```

En `main.py`:

```python
from star import router as star_router
app.include_router(star_router)
```

## Decisiones clave (detalle en el documento)

* **ETL = Athena CTAS, reconstrucción completa en cada refresh.** Idempotente por construcción
  (cada corrida escribe en `star/<tabla>/v=<run_id>/` y borra las versiones viejas), sin clúster,
  ~1 min por corrida, ~US$0,0004 por corrida (7 consultas facturables x 10 MB mínimo).
* **La landing zone se lee como texto** y cada objeto JSON se extrae con regex: funciona con JSON
  por línea, con JSON pegados (Firehose sin delimitador / replay del harness) y con `.gz`.
* **Deduplicación** por `event_id` (`row_number()`), fecha y llaves por `event_ts`, ruta por *as-of join*
  (`valid_from <= fecha < valid_to`). Rutas o clases desconocidas van al miembro `-1`, no se pierden.
* **Estado del refresh en archivo con locks**: funciona con `uvicorn --workers 2`.
