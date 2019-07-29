package com.sundogsoftware.spark.Ratings.sparkSql

import com.sundogsoftware.spark.Ratings.sparkSql.SimplePartitionTutorial.People
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

import scala.util.Try


object  GroupBySQLExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  def getNumberOfColumns(df : DataFrame) = {
    df.columns.length
  }

  val peopleList = List(
    People("Srinivas", 37, "blue","M"),
    People("Kirthika", 37, "pink","F"),
    People("Sadhana", 6, "blue","F"),
    People("Sachin", 30, "blue","M"),
    People("Sadhiv", 1, "black","M"),
    People("Kamila", 20, "rose","F")
  ).toDF("name","age","colour","sex")

  peopleList.schema.fields.filter(x => x.name == "age").map(x => (x.name,x.dataType)).foreach{ case(x,y) => println(x + " " +  y)}

  peopleList.printSchema()



  val groups = peopleList.groupBy("sex").agg(count("*"))

  groups.show()



  def getNumberOfColumns(inputFolder : String) = {
    val df = Try(spark.read.parquet(inputFolder))
    val output = df match {
      case scala.util.Success(value) => (inputFolder, value.columns.length)
      case scala.util.Failure(exception) => (inputFolder, 0)
    }
    output
  }


  val peopleListWithDupls = List(
    People("Srinivas", 37, "blue","M"),
    People("Kirthika", 37, "pink","F"),
    People("Sadhana", 6, "blue","F"),
    People("Sachin", 30, "blue","M"),
    People("Sadhiv", 1, "black","M"),
    People("Sadhiv", 2, "white","M"),
    People("Sadhiv", 3, "blue","M"),
    People("Kamila", 20, "rose","F")
  ).toDF("name","age","colour","sex")

  peopleListWithDupls.dropDuplicates("name").show()

  val output = spark.sparkContext.getPersistentRDDs


  Seq(1451690560979L).toDF("time").withColumn("min_actual_timestamp", from_unixtime($"time" / 1000).cast(TimestampType)).show()

}
