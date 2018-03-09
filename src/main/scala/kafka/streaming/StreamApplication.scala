package main.scala.kafka.streaming

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.KStreamBuilder

/**
  * 1-Create topics
  * kafka-topics --zookeeper localhost:2181 --create --topic SourceTopic  --partitions 1 --replication-factor 1
  * kafka-topics --zookeeper localhost:2181 --create --topic SinkTopic  --partitions 1 --replication-factor 1
  *
  * 2-Start Confluent
  * confluent start
  *
  * 3-Produce message into SourceTopic
  * kafka-avro-console-producer --broker-list localhost:9092 --topic SourceTopic  --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'
  * message
  * {"f1":"value1"}
  *
  * 4-start StreamApplication (locally or in a docker container)
  *
  * 5- Consume message
  * kafka-avro-console-consumer --topic SinkTopic         --zookeeper localhost:2181          --from-beginning
  */
object StreamApplication {
  def main(args: Array[String]): Unit = {
    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.33.221:9092")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties
    }

    val builder = new KStreamBuilder()
    val sourceStream = builder.stream("SourceTopic")
    sourceStream.to("SinkTopic")

    val streams = new KafkaStreams(builder, config)
    streams.start()
  }
}
