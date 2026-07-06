from pyspark.sql import SparkSession
import config.settings as settings_module


def create_spark_session():
    return SparkSession.builder \
        .appName("AQI-Bronze-Layer") \
        .master("local[*]") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.hadoop.fs.s3a.access.key", settings_module.AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", settings_module.AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .getOrCreate()





# HOW spark_session.py WORKS:
# config/settings.py loads AWS credentials from .env
# → create_spark_session() called by bronze/main.py
# → SparkSession built with local[*] — uses all CPU cores on your machine
# → spark.jars.packages downloads 3 jars on first run (cached after):
#     → spark-sql-kafka: lets PySpark read from Kafka
#     → hadoop-aws: lets PySpark write to S3 using s3a:// protocol
#     → aws-java-sdk-bundle: AWS SDK that hadoop-aws needs to talk to S3
# → fs.s3a.access.key + fs.s3a.secret.key — AWS credentials passed to Hadoop
# → fs.s3a.endpoint set to ap-south-1 (Mumbai) — matches your S3 bucket region
# → fs.s3a.impl — tells Hadoop to use S3AFileSystem for all s3a:// paths
# → KryoSerializer enabled — faster data transfer between Spark workers
# → .getOrCreate() — reuses existing session if already running
# → Returns spark object — kafka_consumer.py and s3_writer.py use this same object