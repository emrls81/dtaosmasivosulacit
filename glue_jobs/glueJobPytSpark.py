import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


DATABASE_NAME = "datosmasivos_db"
TABLE_NAME = "raw"

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE_NAME,
    table_name=TABLE_NAME
)

df = datasource0.toDF()


from pyspark.sql.functions import col, to_timestamp, year, month, count as spark_count

df = df.withColumn(
    "start_time_ts",
    to_timestamp(col("start_time"))
)

df = df.filter(col("start_time_ts").isNotNull())
df = df.filter(col("state").isNotNull())

df = df.withColumn("year", year(col("start_time_ts")))
df = df.withColumn("month", month(col("start_time_ts")))


agg_df = df.groupBy("state", "year").agg(
    spark_count("*").alias("total_accidents")
)


OUTPUT_PATH = "s3://datosmasivosulacit/processed/accidents_parquet/"

(
    agg_df
    .write
    .mode("overwrite")
    .partitionBy("year")
    .parquet(OUTPUT_PATH)
)


job.commit()
