package main.scala.spark.streaming

object HdfsWordCount {
  def main(args: Array[String]) {
 /*   if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //wordCounts.print()
    wordCounts.saveAsTextFiles("/tmp/HdfsCount.txt");
    ssc.start()
    ssc.awaitTermination()*/
  }
}