package main.scala.spark.streaming

import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate
    val ssc = new StreamingContext(sc, Seconds(5))

    import org.apache.spark.streaming.kafka010._

    //val preferredHosts = LocationStrategies.PreferConsistent
    //val topics = List("topic1", "topic2", "topic3")
    val topics = List("test")
    import org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    import org.apache.kafka.common.TopicPartition
    val offsets = Map(new TopicPartition("test", 0) -> 2L)

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))


    dstream.foreachRDD { rdd =>
      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }
    }

    ssc.start

    // the above code is printing out topic details every 5 seconds
    // until you stop it.

    ssc.stop(stopSparkContext = false)
  }
}