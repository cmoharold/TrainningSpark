package com.harold.cca175.sparkStreaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object BasicWordCountStreaming {

  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("BasicWordCountStreaming")
    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    // Escribir datos en el puerto 9999 con: nc -l 9999
    val lines = ssc.socketTextStream("localhost",9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordcounts = pairs.reduceByKey(_+_)

    wordcounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}