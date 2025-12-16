SELECT COUNT(*) AS total_rows
FROM datosmasivos_db.raw;

SELECT *
FROM datosmasivos_db.raw
LIMIT 20;

///////////////////////////////

CREATE TABLE datosmasivos_db.accidentes_parquet
WITH (
  format = 'PARQUET',
  external_location = 's3://datosmasivosulacit/processed/accidents_parquet/',
  partitioned_by = ARRAY['year']
) AS
SELECT *,
       year(try_cast(start_time AS timestamp)) AS year
FROM datosmasivos_db.raw
WHERE try_cast(start_time AS timestamp) IS NOT NULL;

//////////////////////////////////

SELECT COUNT(*) AS total_rows
FROM datosmasivos_db.accidentes_parquet;

SELECT year, COUNT(*) AS total
FROM datosmasivos_db.accidentes_parquet
GROUP BY year
ORDER BY year;

///////////////////////////////