package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._



object StatefulNetworkWordCount {

  def getPerson (str : String) : Persons = {
    val splitArray = str.split(",")
    val name = splitArray(0)
    val school = splitArray(1)
    val timestamp = StatefulNetworkWordCountPersonPerson.getTimestamp(splitArray(2))
    Persons(name, school, timestamp)
  }

  //Now, newValues is the new set of values
  //runningCount is the existing values for each key

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {

    println("New values ..." + newValues.sum)
    println("Running count values ..." + runningCount.getOrElse(0))
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }


  def updateFunctionFrist(newValues: Seq[String], runningCount: Option[String]): Option[String] =
  {
    println("Inside function ... ")
    println("New values ..." + newValues.headOption.getOrElse(""))
    println("Running count values ..." + runningCount.getOrElse(""))
    val newWord = if (runningCount.getOrElse("") == "") //Check if running count is nothing for a key
      {
        val str = newValues.head.toString //Use existing values
        Some(str)
      }
    else
    {
      val str = runningCount.getOrElse(newValues.head.toString) //Use new values
      Some(str)
    }
    newWord
  }

  def updateFunctionFirstPerson(newValues: Seq[Person], state: Option[Person]): Option[Person] =
  {
    println("Inside function ... ")
    println("New values ..." + newValues.headOption.getOrElse(""))
    println("Running count values ..." + state.getOrElse(""))
    val newWord = if (state.getOrElse("") == "") //If running count is empty
    {
      val str = newValues.head.asInstanceOf[Person]
      Some(str)
    }
    else
    {
      val str = state.getOrElse(newValues.head.asInstanceOf[Person])
      Some(str)
    }
    newWord
  }


  def main(args: Array[String]) {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[8]").setAppName("StatefulNetworkWordCount")


    val ssc = new StreamingContext(conf, Seconds(10))

    // Set checkpoint directory
    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = words.map(word => (word.hashCode, word))

    // Update state using `updateStateByKey`
    //val runningCounts = pairs.updateStateByKey[String](updateFunction _)


    val runningCounts = pairs.updateStateByKey[String](updateFunctionFrist _)


    // Print the first ten elements of each RDD generated in this DStream to the console
    //runningCounts.foreachRDD((rdd,time) => if (rdd.count() > 4) print(rdd))

    //printing only when the counts go above 2
  //runningCounts.foreachRDD(rdd => rdd.foreach(x => if (x._2 > 2) println(x._1, x._2)))

    runningCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}