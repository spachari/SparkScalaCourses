package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


case class C(s1: String*)

object GetACollectionOfColumnsFromADataFrame extends App {


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

  val customerDF = Seq(
    ("C1", "Jackie Chan", 50, "Dayton", "M"),
    ("C2", "Harry Smith", 30, "Beavercreek", "M"),
    ("C3", "Ellen Smith", 28, "Beavercreek", "F"),
    ("C4", "John Chan", 26, "Dayton","M")
  ).toDF("cid","name","age","city","sex")

  val c = C("Srinivas", "pachari")

  val sequence = Seq(col("cid"), col("name"))

  //val output = sequence : _
  customerDF.select(sequence :_*)

  def printFruits(i : String*) = {
    i.foreach(println)
  }

  val fruits = List("apple", "mango", "banana")

  printFruits(fruits :_*)

  fruits.map(x => x)

}
