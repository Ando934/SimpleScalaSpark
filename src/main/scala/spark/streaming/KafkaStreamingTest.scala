package main.scala.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object KafkaStreamingTest {
  def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .getOrCreate()

    // Create schema
    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    //Create the Streaming DataFrame
    val streamingDataFrame = spark.readStream.schema(mySchema).csv("/user/tandrian/ingestion/streaming/")

    // Publish stream to kafka
    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("topic", "test")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/tandrian/spark/checpoint")

    // Suscribe stream from Kafka

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

        // Print
    df.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
}