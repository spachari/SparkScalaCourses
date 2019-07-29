package com.sundogsoftware.spark.Ratings

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Persons(name : String, school : String, timestamp : DateTime)



object StatefulNetworkWordCountPersonPerson {


  def getTimestamp(timestampString : String) : DateTime =
  {
    val sourceFormat = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
    val timestampLongValue = timestampString.toLong
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val timestamp = timestampFormat.format(timestampLongValue)
    DateTime.parse(timestamp, sourceFormat)
  }

  def getTimestampInMilli(timestampString : String) : DateTime =
  {
    val sourceFormat = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss.SSS")
    val timestampLongValue = timestampString.toLong
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS")
    val timestamp = timestampFormat.format(timestampLongValue)
    DateTime.parse(timestamp, sourceFormat)
  }

  def getPerson (str : String) : Persons = {
    val splitArray = str.split(",")
    val name = splitArray(0)
    val school = splitArray(1)
    val timestamp = getTimestamp(splitArray(2).trim)
    println(name)
    println(school)
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

  def getInt(f : Seq[Int] => Int, seq : Seq[Int]) = {
    val output = f(seq)
    output
  }



  def updateFunctionVal(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[(Int)] = {

    val result = if (newValues.isEmpty) { //check if the key is present in new batch if not then return the old values
      Some(runningCount.get)
    }
    else {
      runningCount match {
        case x if runningCount.isEmpty => Some(newValues.fold(0)(_ + _))
        case _ => Some(newValues.fold(0)(_ + _) + runningCount.get)
      }
    }
    result
  }

  def updateFunctionFrist(newValues: Seq[String], runningCount: Option[String]): Option[String] =
  {
    println("Inside function ... ")
    println("New values ..." )
    newValues.foreach(println)
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



  //This job is run for each value that comes in an RDD
  def updateFunctionFirstPerson(newValues: Seq[Persons], state: Option[Persons]): Option[Persons] =
  {
    println("Inside function ... ")
    println("New values ...")
    newValues.foreach(println)
    println("Running count values ..." + state.getOrElse(""))

    val newWord = if (state.getOrElse("") == "") //Check if existing state is nothing for a key i.e. if this is the first time a value is present
    {
      val str = newValues.head.asInstanceOf[Persons] //Take the first value for the value
      Some(str)
    }
    else
    {
      val str = state.head //.getOrElse(newValues.head.asInstanceOf[Persons]) //Take the value from the state
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
    //val words = lines.flatMap(_.split(" "))

    // Count each word in each batch
    val pairs = lines.map(word => (word.hashCode, getPerson(word)))


    // Update state using `updateStateByKey`
    //val runningCounts = pairs.updateStateByKey[String](updateFunction _)


    val runningCounts = pairs.updateStateByKey[Persons](updateFunctionFirstPerson _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    //runningCounts.foreachRDD((rdd,time) => if (rdd.count() > 4) print(rdd))

    //printing only when the counts go above 2
    //runningCounts.foreachRDD(rdd => rdd.foreach(x => if (x._2 > 2) println(x._1, x._2)))

    runningCounts.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}