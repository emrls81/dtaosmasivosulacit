## Estructura S3 utilizada

s3://datosmasivosulacit/
- raw/               (CSV original 2.8GB)
- processed/         (salidas en Parquet)
  - accidents_parquet/   (Parquet FULL particionado por year)
- athena-results/    (resultados de queries de Athena)