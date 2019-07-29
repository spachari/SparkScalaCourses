package com.sundogsoftware.spark.Ratings.sparkSql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{to_date}

case class Student(name : String, dateJoined : String)

object StringToDateConversionAndUDF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  val df = spark.createDataFrame(List(
    Student("Srinivas","01-01-2000"),
    Student("Kirthika","04-07-2011"),
    Student("Sadhana","01-04-2013"),
    Student("Sadhiv","27-12-2018"))).toDF("name","dateJoined")

  val dfyyyyMMdd = spark.createDataFrame(List(Student("Srinivas","2000-01-01"),
    Student("Kirthika","2011-04-07"),
    Student("Sadhana","2013-04-01"),
    Student("Sadhiv","2018-01-04"))).toDF("name","dateJoined")

    //If you want to use col, we need to import them
    //Not working to_date only works for
   // val output = df.select(col("name"),to_date($"dateJoined","MM/dd/yyyy"))


  //To date works for timestamp format 2011-04-07
  val output = dfyyyyMMdd.select(col("name"),to_date(col("dateJoined")))

    output.show()

  import org.joda.time.DateTime

  import org.joda.time.LocalDate

  val d = LocalDate.parse("2018-06-01")
  val dPlusTen = d.plusDays(10)

  println(s"${d} --- ${d.plusDays(10)}")

  val currentDate= new DateTime("2018-01-01")


}
