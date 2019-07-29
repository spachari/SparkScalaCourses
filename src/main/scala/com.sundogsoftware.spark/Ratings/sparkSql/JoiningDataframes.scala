package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


case class Purchase(year : Int,
                    cid : String,
                    isbn : String,
                    shop : String,
                    price : Int)

case class Customer(cid : String,
                    name : String,
                    age : Int,
                    city : String,
                    genre : String)

case class Book (isbn : String, genre : String)

object JoiningDataframes extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("partitionTutorial")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()


  import spark.implicits._

  val purchaseDF = Seq(
    (1999, "C1", "B1", "Amazon", 90),
    (2001, "C1", "B2", "Amazon", 20),
    (2008, "C2", "B2", "Barnes Noble", 30),
    (2008, "C3", "B3", "Amazon", 28),
    (2009, "C2", "B1", "Borders", 90),
    (2010, "C4", "B3", "Barnes Noble", 26)
  ).toDF("year", "cid","isbn","shop","price")

  val customerDF = Seq(
    ("C1", "Jackie Chan", 50, "Dayton", "M"),
    ("C2", "Harry Smith", 30, "Beavercreek", "M"),
  ("C3", "Ellen Smith", 28, "Beavercreek", "F"),
  ("C4", "John Chan", 26, "Dayton","M")
  ).toDF("cid","name","age","city","sex")

  val addressDF = Seq(
    ("C1", "Jackie Chan", 50, "Dayton", "M"),
    ("C2", "Harry Smith", 30, "Beavercreek", "M"),
    ("C3", "Ellen Smith", 28, "Beavercreek", "F"),
    ("C4", "John Chan", 26, "Dayton","M")
  ).toDF("cid","name","age","city","sex")

  val bookDF = Seq(
    ("B1","Novel"),
    ("B2","Drama"),
    ("B3","Poem")
  ).toDF("isbn","genre")

  val longDF = Seq(
    (1539668681883L),
    (1539668681883L)
  ).toDF("number")


  case class Visits (guid : String, milliseconds : Long)

  val guidtests = Seq(
    ("$GuidA", 1539668681883L),
    ("$GuidA", 1539668581883L),
    ("$GuidA", 1539668481883L),
    ("GuidA", 1539678681883L),
    ("GuidA", 1539678581883L),
    ("GuidA", 1539678481883L),
    ("GuidA", 1539698681883L),
    ("GuidA", 1539698581883L),
    ("GuidA", 1539698481883L)
  ).toDF("guid", "milliseconds")

  val output = purchaseDF.join(customerDF, purchaseDF("cid") === customerDF("cid")).join(bookDF, purchaseDF("isbn") === bookDF("isbn"))
      .drop("cid")

  output.show()

  guidtests.where($"guid" === "GuidA" && $"milliseconds" === 1539678481883L).show()

  val guidTestsDS = guidtests.as[Visits]

  guidTestsDS.where($"guid" === "GuidA" && $"milliseconds" === 1539678481883L).show()

  guidtests.where ($"guid" === "$GuidA").show()

  //Simple udf

  val myUDF :(String) => String = (x: String) => x.toUpperCase

  import org.apache.spark.sql.functions.udf
  val upperUDF = udf(myUDF)

  println(scala.util.Random.nextInt(5))

  val randomNumberPicker : String => String = (s : String) => {
  val list = List("apple","Orange","banana","Strawberry","melon")
    list(scala.util.Random.nextInt(5))
  }

  val randomNumberPickerUDF = udf(randomNumberPicker)

  guidtests
    .withColumn("myUDF_out", upperUDF($"guid"))
    .withColumn("random_value", randomNumberPickerUDF($"guid")).show()


  def sqlEscape(s: String) =
    org.apache.spark.sql.catalyst.expressions.Literal(s).sql

  println(sqlEscape("'Ulmus_minor_'Toledo' and \"om\""))
  println(sqlEscape("$parallax$4SG+s0rtEcxu2aPOYquWCCY5gUbNjKAUI8YhsUdDzlY"))

}
