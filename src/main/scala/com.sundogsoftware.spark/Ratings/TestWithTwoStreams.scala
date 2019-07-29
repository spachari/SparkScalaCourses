package com.sundogsoftware.spark.Ratings



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}




object TestWithTwoStreams {

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

  case class Timestamps (mins : Long, maxs : Long)

  def mergeValue(acc : Timestamps, value : Person1) = {
    val mini = acc.mins min value.time
    val maxi = acc.maxs max value.time
    Timestamps(mini, maxi)
  }

  def mergeCombiner(time1 : Timestamps, time2 : Timestamps) = {
    val mini = time1.mins min time2.mins
    val maxi = time1.maxs max time2.maxs
    Timestamps(mini, maxi)
  }

  case class OutputRDD (name : String,
                        firstSalary : String,
                        lastDegree : String,
                        maxTime : Long,
                        minTime : Long,
                        number : Long,
                        count_in_list : Long,
                        countsBoolean : Boolean)

  case class Outputs(name : String,
                     school : String,
                     defaultMinValue : Long,
                     defaultMaxValue : Long,
                     counter : Long,
                     degree : Option[String])


  val mappingFunc = (key: PersonName, value_one: Option[AggregatedPerson], state: State[Outputs]) => {

    val defaultPerson = Outputs("srinivas", "", 1L, Long.MaxValue, 0, Some("str"))

    //val defaultPerson = OutputValues( "", 1L, Long.MaxValue, 0, Some("str"))

    val defaultValues  =
      state
        .getOption()
        .getOrElse(defaultPerson)


    println("value in value_one ... " + value_one.getOrElse(0))
    println("value in state ... " + state.getOption().getOrElse(defaultPerson))

    val maxi = value_one.get.maxTime max defaultValues.defaultMinValue
    val mini = value_one.get.minTime min defaultValues.defaultMaxValue

    val first = if (!value_one.get.firstSchool.isEmpty && defaultValues.school == "")
    {
      value_one.get.firstSchool
    }
    else
    {
      defaultValues.school
    }

    val counter = if (defaultValues.counter == 0) defaultValues.counter + 1 else value_one.get.counts + 1

    val last = if (defaultValues.degree.get == "str" && value_one != None) //First non null value
    {
      Some(value_one.get.last_degree)
    }
    else if (defaultValues.degree.get != "str" && value_one != None) //An update for a non null value
    {
      Some(value_one.get.last_degree)
    }
    else if (value_one == None)
    {
      Some(defaultValues.degree.get)
    }
    else
    {
      Some(defaultValues.degree.get)
    }



    //println("defaultValues.degree.get " + defaultValues.degree.get)
    //println("value_one.get.last_degree " + value_one.get.last_degree)
    //println("value_one.get.last_degree " + value_one.get.last_degree.getClass)
    //println("value_one.get.last_degree " + value_one.getClass)
    println("last " + last)

    val output = Outputs(key.name, first, maxi, mini, counter, last)

    state.update(output)

    output
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[8]").setAppName("test")


    val ssc = new StreamingContext(conf, Seconds(10))

    ssc.checkpoint(".")

    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    val lines1 = ssc.socketTextStream("localhost", 9999)

    lines.foreachRDD{ println("*** lines DStream ... ")
      x : RDD[String] => x.foreach( x => println("lines " + x))}

    lines1.foreachRDD{println("*** lines DStream ... ")
      x : RDD[String] => x.foreach( x => println("lines1 " + x))}

    val outputDStream = lines.union(lines1)

    // Split each line into words
    val words = lines.map(x => getPerson(x))

    words.foreachRDD{println("*** union DStream ... ")
      x : RDD[Person1] => x.foreach( x => println("union " + x))}

    def isInList(a : Column) = {
      val list = List("SCHOOL1")
      val x = a.toString()
      if (list.contains(x)) true else false
    }



    val list = List("School1")


    val df = words.transform {
      rdd =>

        val  makeDT = (list: String) =>
        {
          s"${list}"
        }

        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        spark.udf.register("makeDT", makeDT(_:String))

        import spark.implicits._
        val partitionWindow = Window.partitionBy($"name").orderBy($"time")
        val partitionWindowRemoveNulls1 = Window.partitionBy($"name").orderBy($"time".desc)
        val partitionWindowRemoveNulls2 = Window.partitionBy($"name").orderBy($"time")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        val df = rdd.toDF("name","school", "time", "degree") // create new dataframe

        val filteredLastNameDF = df.filter($"degree".isNotNull).withColumn("lastDegree", first("degree") over (partitionWindowRemoveNulls1)).drop("school", "time","degree").distinct

        //filteredDF.show()

        val windowDF = df.withColumn("firstSalary", first("school") over (partitionWindow))
          .withColumn("lastDegree", last("degree", true) over (partitionWindowRemoveNulls2))


        //val joinedDF = windowDF.join(filteredLastNameDF, windowDF.col("name") === filteredLastNameDF.col("name")).drop(filteredLastNameDF.col("name"))

        val groupedDF =
          windowDF.groupBy("name","firstSalary","lastDegree")
            .agg(min("time").alias("maxTime"),
              max("time").alias("minTime"),
              count("name").alias("number"),
              sum(when(col("school").isin(list:_*), 1).otherwise(0)).alias("count_in_list")
              , sum(when(col("degree").isNotNull, 1).otherwise(0)).alias("counts")
            )//.filter($"lastDegree" =!= "null")

        val outputDF = groupedDF.select("name","firstSalary","lastDegree","maxTime","minTime","number","count_in_list","counts")
          .withColumn("countsBoolean", when($"counts" === 0,true).otherwise(false))
          .drop("counts").as[OutputRDD]

        //outputDF.show()
        outputDF.rdd
    }



    val aggregatedPersonDStream = df.map(x => PersonKV(PersonName(x.name), AggregatedPerson(x.firstSalary, x.lastDegree, x.maxTime, x.minTime, x.number, x.count_in_list, x.countsBoolean)))

    val aggregatedKVDStream = aggregatedPersonDStream.map(x => (x.person, x.personValues))

    //aggregatedKVDStream.print()

    /* Code for combineByKey function
    val pairs = words.map(word => (word.name, Person1(word.name, word.school, word.time, word.degree)))

    val numOfPartitions = 10// specify the amount you want
    val hashPartitioner = new HashPartitioner(numOfPartitions)

    val output = pairs.combineByKey(
      (in : Person1) => Timestamps(in.time, in.time),
      (acc : Timestamps, value : Person1) => mergeValue(acc,value),
      (acc2 : Timestamps , acc3 : Timestamps) => mergeCombiner(acc2, acc3),
      hashPartitioner
    )
   */

    val stateDstream = aggregatedKVDStream.mapWithState(
      StateSpec.function(mappingFunc))
    //.map(x => (x.degree, getTimestamp(x.defaultMaxValue.toString)))

    //stateDstream.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
