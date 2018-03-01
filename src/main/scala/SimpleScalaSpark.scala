/****
  *
  *  Poc project - main class
  *  spark-submit --class main.scala.Main $HOME/spark/lib/simplescalaspark_2.11-0.1.jar \
  *  prd hdfs:///user/tandrian/ingestion/input/shakespeare.txt /tmp/shakespeareWordCount1
  */
package main.scala

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

object SimpleScalaSpark {

  def main(args: Array[String]) {
    // get args
    val executionEnvironment = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    // init props
    val props = ConfigFactory.load()
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster(props.getConfig(executionEnvironment).getString("executionMode"))
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("hdfs:///user/tandrian/ingestion/input/shakespeare.txt")
    val textFile = sc.textFile(inputPath)

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)
    System.out.println("Total words: " + counts.count());
    //counts.saveAsTextFile("/tmp/shakespeareWordCount1");
    counts.saveAsTextFile(outputPath);
  }

}