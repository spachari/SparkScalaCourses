package com.sundogsoftware.spark.Ratings

import org.apache.spark.SparkContext

object ExampleForReadingData extends App {

  def getNumberAfterLine(s : String) = {
    s.substring(5)
  }

  val sc = new SparkContext("local[*]", "Sample")

  val files = sc.parallelize(Seq("I am line 1", ("I am line 2")))

  val numbers = files.map(x => getNumberAfterLine(x))

  files.foreach(println)

  numbers.foreach(println)


}
