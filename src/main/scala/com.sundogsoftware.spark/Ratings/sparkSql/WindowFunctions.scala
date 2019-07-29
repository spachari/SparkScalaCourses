package com.sundogsoftware.spark.Ratings.sparkSql


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class PeopleWIthPref(name : String, age : Int,  preference : Int,  colour : String,  sex : String)

object WindowFunctions extends App {


  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val peopleList = List(
    PeopleWIthPref("Srinivas", 37, 1, "blue", "M"),
    PeopleWIthPref("Srinivas", 37, 2, "black", "M"),
    PeopleWIthPref("Kirthika", 37, 1, "pink", "F"),
    PeopleWIthPref("Kirthika", 37, 2, "magenta", "F"),
    PeopleWIthPref("Sadhana", 6, 1, "blue", "F"),
    PeopleWIthPref("Sachin", 30, 1, "blue", "M"),
    PeopleWIthPref("Sadhiv", 1, 1, "black", "M"),
    PeopleWIthPref("Kamila", 20, 1, "rose", "F")
  ).toDF("name","age","preference","colour","sex")

  val namePartition = Window.partitionBy($"name").orderBy($"preference")

  peopleList.select($"name", $"age", $"colour", $"preference")
    .withColumn("firstColour", first($"colour") over (namePartition))
    .withColumn("lastName", last($"colour") over (namePartition))
    .show()

}
