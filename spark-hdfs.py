from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, explode
from pyspark.sql.types import *

# Create Spark session
spark = SparkSession.builder \
    .appName("COVID Kafka ETL") \
    .getOrCreate()

# Define schema
country_schema = StructType([
    StructField("country", StringType()),
    StructField("cases", LongType()),
    StructField("todayCases", LongType()),
    StructField("updated", LongType()),
    StructField("population", LongType()),
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "covid-data") \
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()

# Parse JSON array
json_array_df = df.select(from_json(col("value").cast("string"), ArrayType(country_schema)).alias("data"))

# Explode
flat_df = json_array_df.select(explode(col("data")).alias("country_data"))

# Extract fields
final_df = flat_df.select(
    col("country_data.country"),
    col("country_data.cases"),
    col("country_data.todayCases"),
    col("country_data.population"),
    to_timestamp((col("country_data.updated") / 1000).cast("timestamp")).alias("updated_time")
)

# ✅ Write to HDFS (Parquet recommended)
query = final_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://localhost:9000/Prathamesh/") \
    .option("checkpointLocation", "hdfs://localhost:9000/Prathamesh/chk/") \
    .start()

query.awaitTermination()
