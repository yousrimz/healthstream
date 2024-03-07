import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from conf import SPARK_JARS_PACKAGES, SPARK_JARS


PYTHON_PATH = os.environ.get("PYSPARK_PYTHON", r"C:\Users\yousr\python.exe")
JARS_DIR = os.environ.get("SPARK_JARS_DIR", r"C:\spark\jars")

# config for AWS

try:
    with open('utils\config.json') as config_file:
        cfg = json.load(config_file)
        aws_access_key_id = cfg['aws_access_key_id']
        aws_secret_access_key = cfg['aws_secret_access_key']
except FileNotFoundError:
    print("config file not found.")
    sys.exit(1)
except KeyError as e:
    print(f"AWS key is missing in config file.: {e}")
    sys.exit(1)


# init spark session
try:
    spark = SparkSession.builder \
        .config("spark.jars.packages", ",".join(SPARK_JARS_PACKAGES)) \
        .config("spark.jars", ",".join([os.path.join(JARS_DIR, jar) for jar in SPARK_JARS])) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .appName("HealthData") \
        .config("spark.network.timeout", "5000s") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
except Exception as e:
    print(f"error when init spark session: {e}")
    sys.exit(1)


# reading kafka stream
try:
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "arhvdata") \
        .load()
except Exception as e:
    print(f"error when reading kafka stream: {e}")
    sys.exit(1)


# def data schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("disease", StringType(), True),
    StructField("measurement_type", StringType(), True),
    StructField("measurement_value", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

try:
    # select key/value columns
    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # writing stream in parquet format on AWS S3 bucket
    query = df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "s3a://datareco/parquets/") \
        .option("checkpointLocation", "C:\\Users\\yousr\\Documents\\healthstream\\checkpoint") \
        .trigger(processingTime="40 seconds") \
        .option("maxRecordsPerFile", 30) \
        .start()

    # waiting stream to end
    query.awaitTermination()
except Exception as e:
    print(f"error when stream process: {e}")
finally:
    spark.stop()