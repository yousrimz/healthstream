from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os

os.environ["PYSPARK_PYTHON"] = r"C:\\Users\\yousr\\python.exe"

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
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0") \
        .config("spark.jars", r"C:\spark\jars\aws-java-sdk-core-1.12.663.jar") \
        .config("spark.jars", r"C:\spark\jars\hadoop-aws-3.2.2.jar") \
        .config("spark.jars", r"C:\spark\jars\aws-java-sdk-bundle-1.12.652.jar") \
        .config("spark.jars", r"C:\spark\jars\aws-java-sdk-s3-1.11.563.jar") \
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
        .option("checkpointLocation", "C:\\Users\\yousr\\Documents\\healthstream\\checkp") \
        .trigger(processingTime="40 seconds") \
        .option("maxRecordsPerFile", 30) \
        .start()

    # waiting stream to end
    query.awaitTermination()
except Exception as e:
    print(f"error when stream process: {e}")
finally:
    spark.stop()