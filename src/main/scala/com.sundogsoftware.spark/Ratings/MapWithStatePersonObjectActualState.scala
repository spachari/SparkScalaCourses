package com.sundogsoftware.spark.Ratings

package com.sundogsoftware.spark.Ratings

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


case class Person1(name : String, school : String, time : Long, degree : Option[String], isTrue : Boolean) {
  override def toString: String = s"${name} ... ${school} ... ${time} ... ${degree} ... ${isTrue}"
}

object MapWithStatePersonObjectActualState {


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

    val isTrue = splitArray(4).toBoolean

    Person1(name, school, timestamp, arr, isTrue)
  }



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
  val mappingFunc = (key: String, value: Option[Person1], state: State[Output]) => {
    val defaultPerson = Output("", "", 1L, Long.MaxValue, 0, Some("str"))


    def updatePerson1(value: Option[Person1]) : Option[Output] = {


      val defaultValues  =
        state
          .getOption()
          .getOrElse(defaultPerson)


      println("value in value ... " + value.getOrElse(0))

      val maxi = value.get.time max defaultValues.defaultMinValue
      val mini = value.get.time min defaultValues.defaultMaxValue

      val first = if (!value.get.school.isEmpty && defaultValues.school == "")
      {
        value.get.school
      }
      else
      {
        defaultValues.school
      }
      val counter = defaultValues.counter + 1


      val last = if (value.get.degree != None) //An update for a non null value
      {
        value.get.degree
      }
      else
      {
        Some(defaultValues.degree.get)
      }


      val output = Output(key, first, maxi, mini, counter, last)

      Some(output)

    }

    /*
    value match  {
      case Some(vs) =>
    val sum = updatePerson1(Some(vs))
    state.update(sum.get)
    //Some((key, sum))
    case _ if state.isTimingOut() => {
      println("Calling isTimeOut")
      (key, state.getOption)
    }
    */

      if (!state.isTimingOut) {
        val sum = updatePerson1(value)
        state.update(sum.getOrElse(defaultPerson))
        None
      }
      else {
        println("Timeout function ... ")
        val sum = state.getOption()
        sum
      }





  }


    //Some(output)



  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulNetworkWordCount")


    val ssc = new StreamingContext(conf, Seconds(10))

    ssc.checkpoint(".")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)

    // Split each line into words
    val words = lines.map(x => getPerson(x))


    // Count each word in each batch
    val pairs = words.map(word => (word.name, word))

    //pairs.print()

    val defaultPerson = Output("", "", 1L, Long.MaxValue, 0, Some("str"))

    val stateDstream = pairs.mapWithState(
      StateSpec.function(mappingFunc).timeout(Seconds(30))).flatMap(x => x)
    //.map(x => (x.degree, getTimestamp(x.defaultMaxValue.toString)))



    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
