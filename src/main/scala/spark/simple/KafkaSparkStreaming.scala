package main.scala.spark.simple

/*
 * /usr/hdp/2.6.3.0-235/spark2/bin/spark-submit --master yarn  --deploy-mode client --driver-memory 512m  --executor-memory 1g --executor-cores 1  --num-executors 1 --class main.scala.spark.streaming.KafkaSparkStreaming simplescalaspark_2.11-0.1.jar
 */


object KafkaSparkStreaming {
  def main(args: Array[String]): Unit = {
  /*  val sc = SparkContext.getOrCreate
    val ssc = new StreamingContext()(sc, Seconds(5))

    import org.apache.spark.streaming.kafka010._

    //val preferredHosts = LocationStrategies.PreferConsistent
    //val topics = List("topic1", "topic2", "topic3")
    val topics = List("test")

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
      PreferConsistentt,
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

    ssc.stop(stopSparkContext = false) */
  }
}