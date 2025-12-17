from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = (
    SparkSession.builder
        .appName("KafkaSparkStreaming")
        .getOrCreate()
)

# Esquema del JSON enviado desde Kafka
schema = StructType() \
    .add("id", IntegerType()) \
    .add("mensaje", StringType())

df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")
         .option("subscribe", "mi_tema")
         .load()
)

df_parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
      .select("data.*")
)

query = (
    df_parsed.writeStream
             .format("console")
             .outputMode("append")
             .start()
)

query.awaitTermination()
