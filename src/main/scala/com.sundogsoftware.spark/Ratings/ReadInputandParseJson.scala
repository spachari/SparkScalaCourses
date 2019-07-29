package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._

object StatefulNetworkWordCountPerson {

  def getPerson (str : String) : Persons = {
    val splitArray = str.split(",")
    val name = splitArray(0)
    val school = splitArray(1)
    val timestamp = StatefulNetworkWordCountPersonPerson.getTimestamp(splitArray(2))
    println(name + " -- " + school)
    Persons(name, school, timestamp)

  }

  def getPersonJson(str : String) : Option[Persons] = {
    val persons = getPerson(str)
    val jsonPerson = try {
      val person = ("person" ->
        ("name" -> persons.name) ~
          ("school" -> persons.school)
        )
      println((person))
      Some(person.asInstanceOf[Persons])
    }
    catch {
      case e: Exception => None
    }
    jsonPerson
  }

  //Now, newValues is the new set of values
  //runningCount is the existing values for each key


  def main(args: Array[String]) {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[8]").setAppName("StatefulNetworkWordCount")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/persons")

    // Split each line into words
    //val words = lines.flatMap(_.split(" "))

    val persons = lines.flatMap(x => getPersonJson(x))

    println("Printing json output ... ")
    // Count each word in each batch
    //val pairs = persons.map(word => (persons.hashCode, persons))

    persons.foreach(println)
  }

}