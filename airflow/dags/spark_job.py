import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import types
import argparse
import os
from pyspark.sql.functions import split, col, date_trunc

SERVICE_ACCOUNT_JSON_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
BQ_DATASET_PROD = os.environ.get('BIGQUERY_DATASET', 'crimes_prod')
TMP_BUCKET = os.environ.get('TMP_BUCKET', "dtc_data_lake_secret-meridian-414302" ) #                                                                                   

SPARK_GCS_JAR = "/opt/airflow/lib/gcs-connector-hadoop3-2.2.5.jar"
SPARK_BQ_JAR = "/opt/airflow/lib/spark-bigquery-latest_2.12.jar"


schema = types.StructType(
    [
        types.StructField("time", types.TimestampType(), True),
        types.StructField("latitude", types.FloatType(), True),
        types.StructField("longitude", types.FloatType(), True),
        types.StructField("depth", types.FloatType(), True),
        types.StructField("mag", types.FloatType(), True),
        types.StructField("magType", types.StringType(), True),
        types.StructField("nst", types.FloatType(), True),
        types.StructField("gap", types.FloatType(), True),
        types.StructField("dmin", types.FloatType(), True),
        types.StructField("rms", types.FloatType(), True),
        types.StructField("net", types.StringType(), True),
        types.StructField("id", types.StringType(), True),
        types.StructField("updated", types.TimestampType(), True),
        types.StructField("place", types.StringType(), True),
        types.StructField("type", types.StringType(), True),
        types.StructField("horizontalError", types.FloatType(), True),
        types.StructField("depthError", types.FloatType(), True),
        types.StructField("magError", types.FloatType(), True),
        types.StructField("magNst", types.FloatType(), True),
         types.StructField("status", types.StringType(), True),
        types.StructField("locationSource", types.StringType(), True),
        types.StructField("magSource", types.StringType(), True)
    ]
)

enrich_schema = types.StructType(
    [
        types.StructField("time", types.TimestampType(), True),
        types.StructField("latitude", types.FloatType(), True),
        types.StructField("longitude", types.FloatType(), True),
        types.StructField("depth", types.FloatType(), True),
        types.StructField("mag", types.FloatType(), True),
        types.StructField("magType", types.StringType(), True),
        types.StructField("nst", types.FloatType(), True),
        types.StructField("gap", types.FloatType(), True),
        types.StructField("dmin", types.FloatType(), True),
        types.StructField("rms", types.FloatType(), True),
        types.StructField("net", types.StringType(), True),
        types.StructField("id", types.StringType(), True),
        types.StructField("updated", types.TimestampType(), True),
        types.StructField("place", types.StringType(), True),
        types.StructField("type", types.StringType(), True),
        types.StructField("horizontalError", types.FloatType(), True),
        types.StructField("depthError", types.FloatType(), True),
        types.StructField("magError", types.FloatType(), True),
        types.StructField("magNst", types.FloatType(), True),
        types.StructField("status", types.StringType(), True),
        types.StructField("locationSource", types.StringType(), True),
        types.StructField("magSource", types.StringType(), True),
        types.StructField("city", types.StringType(), True)
    ]
)

dwh_schema = types.StructType(
    [
        types.StructField("_year", types.FloatType(), True),
        types.StructField("_month", types.FloatType(), True),
        types.StructField("_day", types.FloatType(), True),
        types.StructField("city", types.StringType(), True),
        types.StructField("earthquakes_total_count", types.FloatType(), True),
        types.StructField("max_depth", types.FloatType(), True),
        types.StructField("max_mag", types.FloatType(), True),
        types.StructField("avg_depth", types.FloatType(), True),
        types.StructField("avg_mag", types.FloatType(), True)
    ]
)


def country_enrichment_string_func(row):
    print("inside")
    city = None
    if row is not None and row["place"] is not None:
        city = row["place"]
        if ',' in row["place"]:
            city = row["place"].split(',')[-1].strip()
        
    return (
        row["time"], row["latitude"], row["longitude"], row["depth"], row["mag"], row["magType"], row["nst"], row["gap"], row["dmin"], row["rms"], row["net"], row["id"], row["updated"], row["place"], row["type"], row["horizontalError"], row["depthError"], row["magError"], row["magNst"], row["status"], row["locationSource"], row["magSource"], city)


parser = argparse.ArgumentParser()
parser.add_argument('--input_file', required=True)

args = parser.parse_args()
input_file = args.input_file

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('test') \
    .set("spark.jars", f"{SPARK_GCS_JAR},{SPARK_BQ_JAR}") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_JSON_PATH) \
    .set('temporaryGcsBucket', TMP_BUCKET) \
    .set("viewsEnabled","true") \
    .set("materializationDataset",f"{BQ_DATASET_PROD}")

sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set(
    "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile",
                SERVICE_ACCOUNT_JSON_PATH)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()


df = (
    spark.read.option("header", "true").schema(schema).csv(input_file)
)
df.show()
df.printSchema()

enrich_df = (
    df.withColumn("city", split(col("place"), ",")[-1].cast(types.StringType()))
      .select(enrich_schema.fieldNames())
)
enrich_df.createOrReplaceTempView("enrich_full_data")


enrich_df.write.format('bigquery') \
    .option('table', f"{BQ_DATASET_PROD}.full_data") \
    .option("partitionField", "time") \
    .option("partitionType", "DAY") \
    .option("clusteredFields", "city") \
    .mode('append') \
    .save()

enrich_df.write.format('bigquery') \
    .option('table', f"{BQ_DATASET_PROD}.raw_data") \
    .mode('append') \
    .save()

query = f"""
select 
date_trunc(time, year) as _year, 
date_trunc(time, month) as _month, 
date_trunc(time, day) as _day,
city,
count(*) earthquakes_total_count, 
max(depth) max_depth,
max(mag) max_mag,
avg(depth) avg_depth,
avg(mag) avg_mag,
from {BQ_DATASET_PROD}.raw_data
group by 1,2,3,4;
"""

crimes_dwh_df = spark.read.format("bigquery").option("query", query).load()
crimes_dwh_df.show()

crimes_dwh_df.write.format('bigquery') \
    .option('table', f"{BQ_DATASET_PROD}.crimes_dwh") \
    .option("clusteredFields", "city") \
    .mode('overwrite') \
    .save()