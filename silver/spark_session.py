# PURPOSE — Same as bronze's spark_session.py — creates a PySpark session. But silver doesn't need the Kafka connector jar since it reads from S3, not Kafka.


from pyspark.sql import SparkSession
import config.settings as settings_module


def create_silver_spark_session():
    return SparkSession.builder \
        .appName("AQI-Silver-Layer") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.access.key",
                settings_module.AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key",
                settings_module.AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint",
                "s3.ap-south-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.hadoop.home.dir", "C:\\hadoop") \
        .getOrCreate()