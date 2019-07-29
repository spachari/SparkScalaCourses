package com.sundogsoftware.spark.Ratings

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


case class Person1(name : String, school : String, time : Long, degree : Option[String])

object MapWithStateOnPersonObject {


  def getTimestamp(timestampString : String) : Option[DateTime] =
  {
    val s = try {
      val sourceFormat = DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss")
      val timestampLongValue = timestampString.toLong
      val timestampFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      val timestamp = timestampFormat.format(timestampLongValue)
      println("debug ..." + Some(DateTime.parse(timestamp, sourceFormat)))
      Some(DateTime.parse(timestamp, sourceFormat))
    }
    catch
      {
        case e : Exception => None
      }
    println("debug ... completed" )
    s
  }

  def getPerson2 (str : String) : Persons = {
    val splitArray = str.split(",")
    val name = splitArray(0)
    val school = splitArray(1)
    val timestamp = StatefulNetworkWordCountPersonPerson.getTimestamp(splitArray(2).trim)
    Persons(name, school, timestamp)
  }

  def getPerson (str : String) : Person1 = {
    val splitArray = str.split(",")
    val name = splitArray(0)
    val school = splitArray(1)
    val timestamp = splitArray(2).trim.toLong
    val arr = try{
      Some(splitArray(3).trim.toString)
    }
    catch {
      case e : Exception => None
    }

    Person1(name, school, timestamp, arr)
  }

  case class UserEvents(person : Person)

  case class Output(name : String,
                    school : String,
                    defaultMinValue : Long,
                    defaultMaxValue : Long,
                    counter : Int,
                    degree : Option[String])

  case class OutputValues(
                    school : String,
                    defaultMinValue : Long,
                    defaultMaxValue : Long,
                    counter : Int,
                    degree : Option[String])



  // Update the cumulative count using mapWithState
  // This will give a DStream made of state (which is the cumulative count of the words)
  val mappingFunc = (key: String, value_one: Option[Person1], state: State[Output]) => {

    val defaultPerson = Output("srinivas", "", 1L, Long.MaxValue, 0, Some("str"))

    //val defaultPerson = OutputValues( "", 1L, Long.MaxValue, 0, Some("str"))

    val defaultValues  =
      state
        .getOption()
            .getOrElse(defaultPerson)


    println("value in value_one ... " + value_one.getOrElse(0))
    println("value in state ... " + state.getOption().getOrElse(defaultPerson))

    //val newValuesList =

    val maxi = value_one.get.time max defaultValues.defaultMinValue
    val mini = value_one.get.time min defaultValues.defaultMaxValue

    val first = if (!value_one.get.school.isEmpty && defaultValues.school == "")
      {
        value_one.get.school
      }
    else
    {
      defaultValues.school
    }
    val counter = defaultValues.counter + 1
    val last = if (defaultValues.degree.get == "str" && value_one.get.degree != None) //First non null value
      {
        value_one.get.degree
      }
    else if (defaultValues.degree.get != "str" && value_one.get.degree != None) //An update for a non null value
      {
        value_one.get.degree
      }
    else if (value_one.get.degree == None)
    {
      Some(defaultValues.degree.get)
    }
    else
    {
      Some(defaultValues.degree.get)
    }

    //val bool =

    println("Minimum time is " + mini)
    println("Maximum time is " + maxi)
    println("First is " + first)

    val output = Output(key, first, maxi, mini, counter, last)

    //state.update(output)

    //val output = OutputValues(first, maxi, mini, counter, last)

    state.update(output)

    output
  }


  def combinePerson(a : Person1, b : Person1) = {
    val maxTimeStamp = a.time max b.time
    val minTimeStamp = a.time min b.time
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[8]").setAppName("StatefulNetworkWordCount")


    val ssc = new StreamingContext(conf, Seconds(10))

    //val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")
    // Create the context with a 1 second batch size
    //val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.map(x => getPerson(x))

    // Count each word in each batch
    val pairs = words.map(word => (word.name, word))

    pairs.print()

    val stateDstream = pairs.mapWithState(
      StateSpec.function(mappingFunc))
      //.map(x => (x.degree, getTimestamp(x.defaultMaxValue.toString)))



    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
