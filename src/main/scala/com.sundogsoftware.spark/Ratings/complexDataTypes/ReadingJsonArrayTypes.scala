package com.sundogsoftware.spark.Ratings.complexDataTypes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.functions._

object ReadingJsonArrayTypes extends App {

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

  //This method gives us a dataframe from json data
  def jsonToDataFrame(json : String, schema : StructType = null): DataFrame = {
    val reader = spark.read //this gives a dataframereader
    Option(schema).foreach(reader.schema)
    reader.json(spark.sparkContext.parallelize(Array(json)).toDS())
  }

  val events = jsonToDataFrame("""
{
  "a": [1, 2]
}
""")

  events.select('a).show()

  events.select('*).show()

  //Getting an element from an array
  //we can use getItem()
  events.select('a.getItem(0)).show()

  val schemaMapType = new StructType().add("a",  MapType(StringType, IntegerType))

  val data= """
{
  "a": {
    "b": 1
  }
}
"""

  val events1 = jsonToDataFrame(data, schemaMapType)

  events1.select('a.getItem("b")).show()

  val data2 = """
{
  "a": [1, 2]
}
"""

  //Using explode to explode an ArrayType
  val events2 = jsonToDataFrame(data2)

  events2.select(explode('a) as 'x).show()

  //Use explode to explode a MapType
  val data3 = """
{
  "a": {
    "b": 1,
    "c": 2
  }
}
"""

  jsonToDataFrame(data3, schemaMapType).select(explode('a) as (Seq("x", "y"))).show()
  //The output does not make sense

  //Collect_list()



}
