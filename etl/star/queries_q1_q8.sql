-- Preguntas de negocio Q1-Q8 sobre el modelo estrella (Athena, base airline_star, workgroup airline-star).
-- Para el documento y para la demo. revenue_cents ya es neto: +precio al reservar, -precio al cancelar.

-- Q1. Ingreso neto por mes y clase tarifaria
SELECT d.year, d.month, c.fare_class, SUM(f.revenue_cents) / 100.0 AS ingreso_neto
FROM fact_booking_event f
JOIN dim_date d       ON f.date_key = d.date_key
JOIN dim_fare_class c ON f.fare_class_key = c.fare_class_key
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Q2. Reservas, cancelaciones y tasa de cancelacion por ruta
SELECT r.route,
       SUM(f.bookings)      AS reservas,
       SUM(f.cancellations) AS cancelaciones,
       ROUND(1.0 * SUM(f.cancellations) / NULLIF(SUM(f.bookings), 0), 4) AS tasa_cancelacion
FROM fact_booking_event f
JOIN dim_route r ON f.route_key = r.route_key
GROUP BY 1
ORDER BY tasa_cancelacion DESC;

-- Q3. Reservas por dia de la semana (lunes a domingo, por nombre)
SELECT d.weekday, d.weekday_name, SUM(f.bookings) AS reservas
FROM fact_booking_event f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1, 2
ORDER BY 1;

-- Q4. Las 10 rutas con mayor ingreso neto
SELECT r.route, SUM(f.revenue_cents) / 100.0 AS ingreso_neto
FROM fact_booking_event f
JOIN dim_route r ON f.route_key = r.route_key
GROUP BY 1
ORDER BY ingreso_neto DESC, r.route
LIMIT 10;

-- Q5. Ingreso neto por mes y categoria de ruta, con la categoria que tenia la ruta EL DIA DEL EVENTO.
-- No hace falta filtrar por fechas aqui: el ETL ya guardo en route_key la version vigente en esa fecha (SCD 2).
SELECT d.year, d.month, r.route_category, SUM(f.revenue_cents) / 100.0 AS ingreso_neto
FROM fact_booking_event f
JOIN dim_route r ON f.route_key = r.route_key
JOIN dim_date d  ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Q5 al reves (para la sustentacion): lo que daria una dimension TIPO 1 (solo la categoria actual).
-- La diferencia con la Q5 de arriba son exactamente los ingresos de enero de las rutas que cambiaron.
SELECT d.year, d.month, cur.route_category AS categoria_actual, SUM(f.revenue_cents) / 100.0 AS ingreso_neto
FROM fact_booking_event f
JOIN dim_route r   ON f.route_key = r.route_key
JOIN dim_route cur ON cur.route = r.route AND cur.is_current
JOIN dim_date d    ON f.date_key = d.date_key
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

-- Q6. Busquedas vs reservas por ruta (conversion). Dos hechos que comparten dim_route (drill-across).
WITH s AS (
    SELECT r.route, COUNT(*) AS busquedas
    FROM fact_search f JOIN dim_route r ON f.route_key = r.route_key
    GROUP BY 1),
b AS (
    SELECT r.route, SUM(f.bookings) AS reservas
    FROM fact_booking_event f JOIN dim_route r ON f.route_key = r.route_key
    GROUP BY 1)
SELECT COALESCE(s.route, b.route) AS route,
       COALESCE(s.busquedas, 0)   AS busquedas,
       COALESCE(b.reservas, 0)    AS reservas,
       ROUND(1.0 * COALESCE(b.reservas, 0) / NULLIF(s.busquedas, 0), 4) AS conversion
FROM s FULL OUTER JOIN b ON s.route = b.route
ORDER BY conversion DESC;

-- Q7. Festivos vs dias normales: busquedas y reservas (totales y promedio por dia con actividad)
WITH s AS (
    SELECT d.is_holiday, COUNT(*) AS busquedas, COUNT(DISTINCT f.date_key) AS dias
    FROM fact_search f JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 1),
b AS (
    SELECT d.is_holiday, SUM(f.bookings) AS reservas
    FROM fact_booking_event f JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY 1)
SELECT s.is_holiday, s.busquedas, b.reservas, s.dias,
       ROUND(1.0 * s.busquedas / s.dias, 1) AS busquedas_por_dia,
       ROUND(1.0 * b.reservas / s.dias, 1)  AS reservas_por_dia
FROM s JOIN b ON s.is_holiday = b.is_holiday;

-- Q8. Reservas en dias de puente vs el resto
SELECT d.is_long_weekend, SUM(f.bookings) AS reservas, COUNT(DISTINCT f.date_key) AS dias
FROM fact_booking_event f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 1;

-- ---------------------------------------------------------------- evidencia para R8 / R10
-- Eventos sucios: copias duplicadas eliminadas y rechazos con su motivo
SELECT COALESCE(SUM(copies - 1), 0) AS copias_duplicadas_eliminadas FROM stg_events;
SELECT reject_reason, COUNT(*) FROM stg_events WHERE reject_reason IS NOT NULL GROUP BY 1;

-- Poda de particiones: comparar "Data scanned" de estas dos en la consola de Athena
SELECT COUNT(*), SUM(results_count) FROM fact_search WHERE month = '2025-01';
SELECT COUNT(*), SUM(results_count) FROM fact_search;

-- SCD 2: las rutas que cambiaron de categoria el 2025-02-01
SELECT route, route_category, valid_from, valid_to, is_current, route_version
FROM dim_route
WHERE route IN (SELECT route FROM dim_route GROUP BY route HAVING COUNT(*) > 1)
ORDER BY route, valid_from;
