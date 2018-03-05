package main.scala.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
 *  Producer Scala Spark
 *   /usr/hdp/2.6.3.0-235/spark2/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --master yarn  --deploy-mode client --driver-memory 512m  --executor-memory 512m --executor-cores 1  --num-executors 1 --class main.scala.spark.streaming.CsvToKafka simplescalaspark_2.11-0.1.jar
 *
 * Consumer shell
 *  kafka-console-consumer.sh --bootstrap-server 0.0.0.0:6667 --topic test
 */


object CsvToKafka {
  def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkSession
      .builder
      .appName("CSV to Kafka")
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

    streamingDataFrame.writeStream
      .format("console")
      .option("truncate","false")
      .start()

    val df = streamingDataFrame
      //.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
      .selectExpr("CAST(id AS STRING) AS key", "struct(*) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "0.0.0.0:6667")
      .option("checkpointLocation", "/user/tandrian/ingestion/checkpoint")
      .option("topic", "test")
      .start()

    df.awaitTermination()

/*// Publish stream to kafka
streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
.writeStream
.format("kafka")
.option("topic", "test")
.option("kafka.bootstrap.servers", "192.168.33.204:9092")
.option("checkpointLocation", "/home/tandrian/spark/checkpoint")*/

// Suscribe stream from Kafka

/*val df = spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "192.168.33.204:9092")
.option("subscribe", "test")
.load()*/


// Print
/*df.writeStream
.format("console")
.option("truncate","false")
.start()
.awaitTermination()*/

/*val query = df
.writeStream
.outputMode("append")
.format("parquet")
.option("path", "/user/tandrian/parquet")
.option("checkpointLocation", "checkpoint")
.start()
.awaitTermination()*/
}
}