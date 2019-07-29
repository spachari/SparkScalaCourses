package com.sundogsoftware.spark.Ratings

import org.apache.log4j._
import org.apache.spark.SparkContext

object WhatDayHadMostPrecipitation {

  def parseLines(line : String) = {
    val splitLine = line.split(",")
    val dayId = splitLine(1)
    val tempRating = splitLine(2)
    val temparature = splitLine(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (dayId, tempRating, temparature)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Min temparatures My Version")

    val linesRDD = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/1800.csv")

    val allTemparatureRDD = linesRDD.map(x => parseLines(x.toString))

    val maxPrecipitationRDD = allTemparatureRDD.filter(x => x._2 == "PRCP")

    val precipitationPerStationIDRDD = maxPrecipitationRDD.map(x => (x._1, x._3.toFloat))

    val maxPrecPerStationID = precipitationPerStationIDRDD.reduceByKey((x,y) => Math.max(x,y))

    val output = maxPrecPerStationID.sortBy(_._2, ascending = false)

    val maxPercipDate = output.take(1)

    val pi = Math.PI
    println(f"$pi%.2f")

    for (c <- maxPercipDate)
    {
      val dayID = c._1
      val minTemparature = c._2
      val formattedTemp = f"$minTemparature%.2f"
      println(s"The maximum precipitation day was is ${dayID}")
    }
  }
}
