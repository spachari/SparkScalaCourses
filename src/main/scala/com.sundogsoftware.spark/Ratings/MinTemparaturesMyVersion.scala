package com.sundogsoftware.spark.Ratings

import org.apache.log4j._
import org.apache.spark.SparkContext


object MinTemparaturesMyVersion {

case class StationDetails (stationId : String, tempRating : String, temparature : Float)

  def parseLines(line : String) = {
    val splitLine = line.split(",")
    val stationId = splitLine(0)
    val tempRating = splitLine(2)
    val temparature = splitLine(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    val stationDetailsRecord : StationDetails = (StationDetails.apply _).tupled(stationId, tempRating, temparature)
    //This is how you convert a case class to a Tuple
    stationDetailsRecord
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Min temparatures My Version")

    val linesRDD = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/1800.csv")

    val allTemparatureRDD = linesRDD.map(x => parseLines(x.toString))
    //allTemparatureRDD.foreach(println)

    val minTemparatureRDD = allTemparatureRDD.filter(x => x.tempRating == "TMIN")
    //val maxTemparatureRDD = allTemparatureRDD.filter(x => x._2 == "TMAX")

    val temparaturePerStationIDRDD = minTemparatureRDD.map(x => (x.stationId, x.temparature.toFloat))



    val minTempPerStationID = temparaturePerStationIDRDD.reduceByKey((x,y) => Math.min(x,y))

    val output = minTempPerStationID.collect()


    val pi = Math.PI
    println(f"$pi%.2f")

    for (c <- output.sorted)
      {
        val stationID = c._1
        val minTemparature = c._2
        val formattedTemp = f"$minTemparature%.2f F"
        println(s"The minimum temparature for station ${stationID} is ${formattedTemp}")
      }
  }
}
