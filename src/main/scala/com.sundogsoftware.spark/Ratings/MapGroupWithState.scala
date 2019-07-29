package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MapGroupWithState extends App {

  case class PersonsDetails( name : String, school : String)

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount").set("spark.driver.allowMultipleContexts", "true")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  val ssc = new StreamingContext(conf, Seconds(10))

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)

  def getPersonDetails(s : String) : PersonsDetails = {
    val personArray = s.split(",")
    PersonsDetails(personArray(0),personArray(1))
  }


  import spark.implicits._

  val word = lines.map(x => getPersonDetails(x)).transform {
    xs =>
      val xa = xs.toDF().as[PersonsDetails]
      xa.select($"name").
        groupBy($"name").
        agg(count("name").alias("hitsInVisit"))

      xa.rdd
  }

  word.print()

  ssc.start()

  ssc.awaitTermination()

}
