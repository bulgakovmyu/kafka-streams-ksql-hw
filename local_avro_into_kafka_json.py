from pyspark.sql import SparkSession

from pyspark.streaming import StreamingContext
from pyspark.sql import types as T

#  Spark :

spark = SparkSession.builder.appName("streamingdata").getOrCreate()

spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")

sc = spark.sparkContext
ssc = StreamingContext(sc, 20)

#  Kafka Topic Details :

KAFKA_TOPIC_NAME_CONS = "copy_of_expedia"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"
KAFKA_TOPIC_NAME_CONS_2 = "copy_of_expedia_json"

schema = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("date_time", T.StringType(), True),
        T.StructField("site_name", T.IntegerType(), True),
        T.StructField("posa_container", T.IntegerType(), True),
        T.StructField("user_location_country", T.IntegerType(), True),
        T.StructField("user_location_region", T.IntegerType(), True),
        T.StructField("user_location_city", T.IntegerType(), True),
        T.StructField("orig_destination_distance", T.LongType(), True),
        T.StructField("user_id", T.IntegerType(), True),
        T.StructField("is_mobile", T.IntegerType(), True),
        T.StructField("is_package", T.IntegerType(), True),
        T.StructField("channel", T.IntegerType(), True),
        T.StructField("srch_ci", T.StringType(), True),
        T.StructField("srch_co", T.StringType(), True),
        T.StructField("srch_adults_cnt", T.IntegerType(), True),
        T.StructField("srch_children_cnt", T.IntegerType(), True),
        T.StructField("srch_rm_cnt", T.IntegerType(), True),
        T.StructField("srch_destination_id", T.IntegerType(), True),
        T.StructField("srch_destination_type_id", T.IntegerType(), True),
        T.StructField("hotel_id", T.LongType(), True),
        T.StructField("partition", T.IntegerType(), True),
    ]
)

avroDF = spark.read.format("avro").load(
    "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/data/expedia"
)

avroDF.write.mode("overwrite").format("json").partitionBy("partition").save(
    "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/data/expedia_json"
)

jsonDF_str = spark.readStream.schema(schema).json(
    "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/data/expedia_json"
)

query = (
    jsonDF_str.selectExpr("to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .partitionBy("partition")
    .outputMode("append")
    .option("topic", KAFKA_TOPIC_NAME_CONS_2)
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS)
    .option(
        "checkpointLocation",
        "/Users/Mikhail_Bulgakov/GitRepo/kafka-streams-ksql-hw/data/spark_ch",
    )
    .start()
)

query.awaitTermination()
