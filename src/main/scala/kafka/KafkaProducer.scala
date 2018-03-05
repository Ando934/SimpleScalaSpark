package main.scala.kafka

object KafkaProducer extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "hdp.andrian.fr:6667")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test"

  /*for(i<- 1 to 2){
    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
    println(record)
    producer.send(record)
  }

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)*/

  val text:Array[String] = Array("1,The Nightmare Before Christmas,1993,3.9,4568",
                            "2,The Mummy,1932,3.5,4388",
                            "3,Orphans of the Storm,1921,3.2,9062")
  for(i<- 0 to 2){
    val textTmp = text(i)
    val record = new ProducerRecord(TOPIC, s"$i", s"$textTmp")
    println(record)
    producer.send(record)
  }

  producer.close()
}
