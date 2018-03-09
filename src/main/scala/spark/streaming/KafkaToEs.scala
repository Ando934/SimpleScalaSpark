package main.scala.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

/*
 *  Producer Scala Spark
 *   /usr/hdp/2.6.3.0-235/spark2/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 --master yarn  --deploy-mode client --driver-memory 512m  --executor-memory 512m --executor-cores 1  --num-executors 1 --class main.scala.spark.streaming.KafkaToHdfs simplescalaspark_2.11-0.1.jar
 *
 * Consumer shell
 *  kafka-console-consumer.sh --bootstrap-server 0.0.0.0:6667 --topic test
 */


object KafkaToEs {
  def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkSession
      .builder
      .appName("Kafka to ES")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_USER, "elastic")
      .config(ConfigurationOptions.ES_NET_HTTP_AUTH_PASS, "somepass")
      .config(ConfigurationOptions.ES_NODES, "192.168.33.201")
      .config(ConfigurationOptions.ES_PORT, "9200")
      .getOrCreate()


    // Create schema
    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))

    // Suscribe stream from Kafka

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.33.204:6667")
      .option("subscribe", "indexation")
      .option("failOnDataLoss", "false")
      .load()

    import spark.implicits._
    val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]


    // Print
    df1.writeStream
      .format("console")
      .option("truncate","false")
      .start()

    val query = df1
      .writeStream
      .outputMode("append")
      .format("es")
      .option("checkpointLocation", "/user/tandrian/compute/checkpoint")
      .start("index1/type1")

    query.awaitTermination()
  }
}