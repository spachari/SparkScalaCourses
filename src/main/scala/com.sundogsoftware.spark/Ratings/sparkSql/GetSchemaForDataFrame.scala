package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructField

case class Person(firstName : String, lastName : String)

case class Data(columnName : String, columnType : String)

object GetSchemaForDataFrame extends App {


  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val df = List(Person("Srinivas", "Pachari"),
    Person("Matt", "Damon")).toDF

  val df1 = List(Person("Srinivas", "Pachari"),
    Person("Matt", "Damon")).toDF

  val bothSchema : Seq[(StructField, StructField)] = df.schema.zip(df1.schema)

  //Map with map
  val list : List[(Int, Int)] = List((1,1), (2,2), (3,3))

  def getCleanedSchema(df : DataFrame) = {
    val name = "df1"
    val df1 = df.schema.fields.map(x =>   (x.name -> x.dataType.toString)).toMap
    df1
  }

  val cleanedSchema = getCleanedSchema(df)

  val cleanedSchema1 = getCleanedSchema(df1)

  println(cleanedSchema)

  println(cleanedSchema1)

  def compareSchemas(df : Map[String, String], df1 : Map[String, String]) = {
    df.equals(df1)
  }

  val output = compareSchemas(cleanedSchema, cleanedSchema1)
  println(output)
}
