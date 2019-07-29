package com.sundogsoftware.spark.Ratings

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

case class FirstNameFriends (firstname : String, friends : String)

object FriendsByFristName {

  Logger.getLogger("org").setLevel(Level.ERROR)

  def parseLine(line : String) =  {
    val firstName = line.split(",")(1)
    val friends = line.split(",")(3).toInt
    //val s : FirstNameFriends = FirstNameFriends(firstName,friends)
    (firstName,friends)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","friends by firstname")

    val linesRDD = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/fakefriends.csv")

    val firstNameAndFriendsRDD  = linesRDD.map(x => parseLine(x.toString))

    val firstNameByFriendsRDD = firstNameAndFriendsRDD.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val averageOfFriendsByNameRDD = firstNameByFriendsRDD.mapValues(x => x._1 / x._2)

    val outputRDD = averageOfFriendsByNameRDD.collect()

    val sortedRDD = outputRDD.toSeq.sortBy(_._1)

    sortedRDD.foreach(println)

  }
}
