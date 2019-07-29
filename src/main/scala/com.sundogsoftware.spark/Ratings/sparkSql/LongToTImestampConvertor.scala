package com.sundogsoftware.spark.Ratings.sparkSql


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object LongToTImestampConvertor extends App {

  //1548947234

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  val df = Seq(1548947234, 1548917234, 1543947234).toDF("Timestamps")

  df.withColumn("TimeStampTYpe", $"Timestamps".cast(TimestampType))
    .withColumn("TimeStampInSecs", from_unixtime($"Timestamps","yyyy-mm-dd HH:mm:ss")).show()


  case class PeopleList(name: String, age : Long, favColour : String, sex : String)

  val peopleList = List(
    PeopleList("Srinivas", 37L, "blue","M"),
    PeopleList("Kirthika", 37L, "pink","F"),
    PeopleList("Sadhana", 6L, "blue","F"),
    PeopleList("Sachin", 30L, "blue","M"),
    PeopleList("Sadhiv", 1L, "black","M"),
    PeopleList("Sadhiv", 2L, "black","M"),
    PeopleList("Sadhiv", 3L, "black","M"),
    PeopleList("Kamila", 20L, "rose","F")
  ).toDF("name", "age","color","sex")

  peopleList.printSchema()

  peopleList.groupBy("name").agg(last("age").as("last_age")).show(false)







  val acquisition_timestamp_tests = List("20180125T000000Z",
  "20180118T000000Z",
  "20180119T000000Z",
  "20180120T000000Z",
  "20180121T000000Z",
  "20180122T000000Z").toDF("acquisition_timestamp")

  acquisition_timestamp_tests.where(col("acquisition_timestamp") < "20181213T000000Z").show()

}
