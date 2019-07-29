package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

object AccumulatorExample extends App {


  def incrementAccumulator(acc : LongAccumulator)  = {
    println("Icremental accumulator called ... ")
    acc.add(1)
    println(acc.value)
  }

  def getContent(str : String) = {
    val splitArray = str.split("::")
    val num = splitArray(0)
    val number = Try(num.toInt)

    val output = number match {
      case Success(i) => i
      case Failure(i) =>
        {
          0;
          incrementAccumulator(blankLinesAccumulator)
        }
    }
    //println(number + " --- " + output + blankLinesAccumulator.value)
    //println("Total blank lines in accumulator = " + blankLinesAccumulator.value)
    output
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("Aggregator example").setMaster("local[*]")

  val sc = new SparkContext(conf)

  val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/ml-10M100K/ratings-with-blank-lines.dat")

  val spark = SparkSession.builder().appName("test").master("yarn").config("spark.driver.maxResultSize","10g")

  println("There are a total of " + lines.partitions.size + " partitions")

  for (x <- lines.partitions)
    {
      println(x)
    }



  //Create an accumulator
  val blankLinesAccumulator = sc.longAccumulator("counter")

  //val ids = lines.map(x => getContent(x))



  val output = lines.map{ x =>
    if (x == "")
    {
      blankLinesAccumulator.add(1)
    }
  else {
    getContent(x)
  }
  }




  output.distinct().foreach(println)

  println("Total lines in accumulator ... " + blankLinesAccumulator.value)
  //lines.map(x => getContent(x)).distinct().sortBy()foreach(println)

}
