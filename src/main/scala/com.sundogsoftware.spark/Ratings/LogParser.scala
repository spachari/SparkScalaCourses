package com.sundogsoftware.spark.Ratings


import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/** Maintains top URL's visited over a 5 minute window, from a stream
  *  of Apache access logs on port 9999.
  */
object LogParser {

  def main(args: Array[String]) {

    //Logger.getLogger("org").setLevel(Level.ERROR)
    // Create the context with a 1 second batch size
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("CountingSheep")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines

    // Create a socket stream to read log data published via netcat on port 9999 locally

    case class Person1(name : String, id : Int)
    val df = sc.parallelize((1 to 10000).toSeq).repartition(10)

    val testStream = ssc.queueStream(mutable.Queue(df))

    testStream.foreachRDD{x => x.foreach(x => println(x))
      ssc.stop(true)
    }


    // Extract the request field from each log line


    // Kick it off
    ssc.start()
    ssc.awaitTermination()

  }
}

