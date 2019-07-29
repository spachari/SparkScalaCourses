package com.sundogsoftware.spark.Ratings

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.Codec

object PopularMovieData {

  var movieMap : Map[Int, String] = Map() //This is how you create an empty map

  //Create an empty map with MovieID and Movies
  def createMovieMap (): Map[Int, String] = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val file = scala.io.Source.fromFile("/Users/spachari/Desktop/Spark-learning/SparkScala/ml-100k/u.item").getLines()

    for (line <- file) {
      var fields = line.split('|')
      if (fields.length > 1) {
        //println(fields(0).toInt -> fields(1))
        movieMap += (fields(0).toInt -> fields(1))
      }
    }
    movieMap
  }


  def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "PopularMovieData")

    //Broadcasting the whole method will then cause the file to be sent to every node in the cluster
  val nameDictionary = sc.broadcast(createMovieMap)

  val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/ml-100k/u.data")

  val movieIds = lines.map(x => x.toString.split("\t")(1).toInt).map(x => (x,1))

  val popularMovieIds = movieIds.reduceByKey((x,y) => x + y)

  val popMovieIds = popularMovieIds.map(x => (x._2, x._1))

    //Sorting separately is the correct thing to do
    val popularMovieIdsSortedByIds = popMovieIds.sortByKey()

    popularMovieIdsSortedByIds.foreach(println)

    //Using the broadcast value as a lookup
    val output = popularMovieIdsSortedByIds.map(x => (nameDictionary.value(x._2),x._2))

    val results = output.collect()

    results.foreach(println)
  }
}
