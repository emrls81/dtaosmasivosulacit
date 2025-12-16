# Solución en la Nube para Procesamiento de Datos Masivos (AWS)

## Dataset
US Accidents (2016–2023) – ~2.8GB, ~7.7M registros (CSV)

## Arquitectura
- Amazon S3 (Data Lake)
- AWS Glue (Crawler + Data Catalog + Glue Job PySpark)
- Amazon Athena (SQL + CTAS a Parquet particionado)
- Power BI Desktop (ODBC a Athena)

## Estructura del repositorio
- sql/ -> Queries de validación y CTAS (Parquet FULL)
- glue_jobs/ -> Script PySpark del Glue Job (ETL agregado)
- config/ -> Configuración y estructura S3 / ODBC
- docs/ -> Informe final en PDF

## Notas
Se intentó habilitar Amazon QuickSight pero se presentaron restricciones de suscripción; se utilizó Power BI como alternativa.
