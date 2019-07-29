package com.sundogsoftware.spark.Ratings.complexDataTypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.functions._

object ReadingJsonInputStructAndMapType extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("partitionTutorial")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val events = spark.sparkContext.parallelize(
    """{"action":"create","timestamp":"2016-01-07T00:01:17Z"}""" :: Nil)

  import spark.implicits._




  //This method gives us a dataframe from json data
  def jsonToDataFrame(json : String, schema : StructType = null): DataFrame = {
    val reader = spark.read //this gives a dataframereader
    Option(schema).foreach(reader.schema)
    reader.json(spark.sparkContext.parallelize(Array(json)).toDS())
  }

  val schema = new StructType().add("a",
    new StructType().add("b", IntegerType).add("c", IntegerType))

  val data = """
    {
     "a": {
        "b": 1,
        "c": 2
        }
    }
     """

  val json = jsonToDataFrame(data, schema)

  json.printSchema()

  json.select($"a").show()

  json.select($"a.b").show()

  //Using a MapType
  //It automatically converts the json into a mapType
  val schema1 = new StructType().add("a", MapType(StringType, IntegerType))

  val json2 = jsonToDataFrame(data, schema1)

  json2.printSchema()

  json2.select($"a").show()

  json2.select($"a.b").show()

  //Doing a select using a struct

  json.select(struct('a) as 'x).show()

  json.select(struct($"a.b") as 'x).show()

  json.select(struct("*") as 'x).show()
}
