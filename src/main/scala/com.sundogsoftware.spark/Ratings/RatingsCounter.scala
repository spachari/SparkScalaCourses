package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {


  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter") //1. local means the job is running locally
                                                            //2. * means the job can use up all the clusters
                                                            //

    // Load up each line of the ratings data into an RDD. It uses textFile on the file and returns an RDD
    val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/ml-100k/u.data")
    //lines is a dataset that contains 1 row for every row in the file
    //Row 1 - 913	209	2	881367150
    //Row 2 - 378	78	3	880056976
    //Row 3 - 880	476	3	880175444
    //Row 4 - 716	204	5	879795543
    //
    //


    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2)) //(2) will return the particular element from the map
    //2
    //3
    //3
    //5

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue()
    //(2, 1)
    //(3, 2)
    //(5, 1)
    //It's type is scala.collection.Map[String,Long]

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)
    //toSeq is used to convert the results to a Seq and then sortBy acts on it

    // Print each result on its own line.
    sortedResults.foreach(println)
  }
}
