/*
 * spark-submit --class ScalaSparkKafka $HOME/spark/lib/simplescalaspark_2.11-0.1.jar prd /user/tandrian/ingestion/input/ /tmp/kafka
 *
 */
package main.scala

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object ScalaSparkKafka {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ScalaSparkKafka env inputPath outputPath")
      System.exit(1)
    }
    val executionEnvironment = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val props = ConfigFactory.load()
    // Create the SparkSession
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master(props.getConfig(executionEnvironment).getString("executionMode"))
      .getOrCreate()
    // Define the Schema
    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))
    // Create the Streaming Dataframe
    //val streamingDataFrame = spark.readStream.schema(mySchema).csv("/user/tandrian/ingestion/input/")
    val streamingDataFrame = spark.readStream.schema(mySchema).csv(inputPath)
    // Publish the stream  to Kafka
    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "topicName")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "outputPath")
      .start()

    // Subscribe the stream from Kafka
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "topicName")
      .load()

    import spark.implicits._
    //Convert Stream according to my schema along with TimeStamp
    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, TimestampType)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    // Print the dataframe on console
    df1.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
}
