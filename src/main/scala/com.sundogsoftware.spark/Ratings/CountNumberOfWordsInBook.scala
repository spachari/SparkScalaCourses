package com.sundogsoftware.spark.Ratings

import org.apache.log4j._
import org.apache.spark.SparkContext

object CountNumberOfWordsInBook extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","counting the number of words")

  val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/book.txt")

  //Difference between map and flatMap
  //scala> lines.take(5).map(x => x.split(" "))
  //res28: Array[Array[String]] = Array(Array(Self-Employment:, Building, an, Internet, Business, of, One),
  // Array(Achieving, Financial, and, Personal, Freedom, through, a, Lifestyle, Technology, Business),
  // Array(By, Frank, Kane), Array(""), Array(""))

  //scala> lines.take(5).flatMap(x => x.split(" "))
  //res29: Array[String] = Array(Self-Employment:, Building, an, Internet, Business, of, One, Achieving, Financial, and, Personal, Freedom, through, a, Lifestyle, Technology, Business, By, Frank, Kane, "", "")
  //It will flatten the results of the map


  /*
  val words = lines.flatMap(x => x.split(" "))

  //Sorting Approach 1: Problem is this countByValue() will create a map object. We want to use only RDDs to do this work
  val wordCount = words.countByValue()

  //wordCount.foreach(println)
  //THis is how you sort a key value pair
  val sortedOutput1 = ListMap(wordCount.toSeq.sortWith(_._2 > _._2):_*).take(20)

  //sortedOutput.foreach(println)

*/

  val commonWords = List("you","to","a","it","that","in","is","the","your","of","because","with",
    "this","they","will","them","and","are","on")

  val onlyWords = lines.flatMap(x => x.split("\\W+")) //In regular expression, this means give me only words and remove ,$? and so on

  //Sorting Approach 2:
  val lowerCaseWords = onlyWords.map(x => x.toLowerCase.trim)

  val meaningfulLowerCaseWords = lowerCaseWords.filter(x => !commonWords.contains(x))

  val countByOnlyWords = meaningfulLowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x + y)

  val sortedOutput2 = countByOnlyWords.map(x => (x._2, x._1)).sortByKey()

  //sortedOutput2.foreach(println)

  for (c <- sortedOutput2) {
    val count = c._1
    val word = c._2
    println(s"$count " + s" $word")
  }



}
