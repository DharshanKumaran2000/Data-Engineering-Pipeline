from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, FloatType, StringType, TimestampType

schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("product_id", IntegerType()) \
    .add("price", FloatType()) \
    .add("timestamp", StringType())

spark = SparkSession.builder \
    .appName("EcommerceStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

data = raw_stream.selectExpr("CAST(value AS STRING)")
parsed = data.select(from_json(col("value"), schema).alias("data")).select("data.*")

final_data = parsed.withColumn("timestamp", col("timestamp").cast(TimestampType()))

query = final_data.writeStream \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/ecommerce") \
    .option("dbtable", "transactions") \
    .option("user", "postgres") \
    .option("password", "password") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

query.awaitTermination()
