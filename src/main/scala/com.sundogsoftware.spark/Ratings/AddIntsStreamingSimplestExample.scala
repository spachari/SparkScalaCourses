package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


case class Nums (a : Int, b : Int)

object AddIntsStreamingSimplestExample extends App {


  def getNums (str : String) : Nums = {
    val splitArray = str.split(",")
    val a = splitArray(0).toInt
    val b = splitArray(1).toInt
    //println(a)
    //println(b)
    Nums(a,b)
  }

  //Functional style
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

  def update(newValues: Seq[(Int)], runningCount: Option[(Int)]) =
    newValues.foldLeft[Option[(Int)]](runningCount)((count, value) => count.map(_ + value))

  def updateFunctionValtest(newValues: Seq[(Int)], runningCount: Option[(Int)]): Int = {

    val result = if (newValues.isEmpty) { //check if the key is present in new batch if not then return the old values
      Some(runningCount.get)
    }
    else {
      runningCount match {
        case x if runningCount.isEmpty => Some(newValues.fold(0)(_ + _))
        case _ => Some(newValues.fold(0)(_ + _) + runningCount.get)
      }
    }
    result.getOrElse(0)
  }

  def updateFunction(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[Int] = {
    newValues
      .reduceOption(_ min _)                // sum of values, or None
      .map(_ min runningCount.getOrElse(1000000000)) // add runningCount or 0 to sum
      .orElse(runningCount)               // if newValues was empty, just return runningCount
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setMaster("local[8]").setAppName("StatefulNetworkWordCounts")


  val ssc = new StreamingContext(conf, Seconds(10))

  // Set checkpoint directory
  ssc.checkpoint(".")

  // Create a DStream that will connect to hostname:port, like localhost:9999
  val lines = ssc.socketTextStream("localhost", 9999)


  val nums = lines.map(x => getNums(x))


  val output1 = nums.map( x => (x.a, x.b)).updateStateByKey(updateFunctionVal _)


  val output = nums.map( x => (x.a, x.b)).updateStateByKey(update _)


  val output2 = nums.map(x => (x.a, x.b)).updateStateByKey(updateFunction _)


  println("output 1 ... ")
  //output1.foreachRDD(x => x.foreach(println))

  println("output 2 ... ")
  output2.foreachRDD(x => x.foreach(println))

  println("------------------------")

  ssc.start()
  ssc.awaitTermination()

}
