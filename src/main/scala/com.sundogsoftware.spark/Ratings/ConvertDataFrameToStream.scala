package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object ConvertDataFrameToStream  {

  def main(args: Array[String]) {

  // Create the context with a 1 second batch size

    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))
    Logger.getLogger("org").setLevel(Level.ERROR)

  case class Person1(name : String, id : Int)
  val rdd = sc.parallelize(Seq(Person1("Srinivas", 1), Person1("Sandeep", 2)))

  val lines = ssc.queueStream(mutable.Queue(rdd))

    lines.foreachRDD( x => x.foreach(println))

  // Kick it off
  ssc.start()
  ssc.awaitTermination()
    ssc.stop()
}
}


