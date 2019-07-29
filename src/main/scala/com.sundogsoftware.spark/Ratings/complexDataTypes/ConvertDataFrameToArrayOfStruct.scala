package com.sundogsoftware.spark.Ratings.complexDataTypes


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object ConvertDataFrameToArrayOfStruct extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  /*
  def jsonToDataFrame(jsonDF : DataFrame, schema : StructType = null): DataFrame = {
    val reader = spark.read //this gives a dataframereader
    Option(schema).foreach(reader.schema)
    reader.json(jsonDF)
  }
  */

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

  customerDF.show(false)

  val sequence = Seq("name", "age", "city", "sex")

  val output = customerDF.select(col("cid"), to_json(struct(sequence.filter(!"cid".equals(_)).map(col):_*)).as("values"))

  val jsonCustomerDF = customerDF.toJSON

  output.printSchema()

  output.show(false)

  jsonCustomerDF.printSchema()

  jsonCustomerDF.show(false)

  customerDF.rdd.map( x=> (x.fieldIndex("name") -> x.fieldIndex("age")))

  val asMap = udf((keys : Seq[String], values : Seq[String]) =>
    keys.zip(values).toMap)


  val keys = array(sequence.map(lit):_*)

  val values = array(sequence.map(col):_*)

  val dfWithMap = customerDF.withColumn("attributes", asMap(keys, values)).drop(sequence: _ *)

  dfWithMap.printSchema()

  dfWithMap.show(false)


  //Creating the datatype as array type

  val customerDFToStruct = Seq(
    ("C1", "Jackie Chan", 50, "Dayton", "M"),
    ("C2", "Harry Smith", 30, "Beavercreek", "M"),
    ("C3", "Ellen Smith", 28, "Beavercreek", "F"),
    ("C4", "John Chan", 26, "Dayton","M")
  ).toDF("cid","name","age","city","sex")

  val schema = StructType(
                    Array(StructField("cid", StringType, true),
                      StructField("name", StringType, true),
                      StructField("age", IntegerType, true),
                      StructField("city", StringType, true),
                      StructField("sex", StringType, true)))

  val df = spark.createDataFrame(customerDFToStruct.rdd, schema)

  df.printSchema()

  df.show(false)

  val data = Seq(
    Row("Molly",20.0, "dog","biscuit"),
    Row("Goody",3.5, "cat","meat"),
    Row("Anty",0.000006, "ant","leaf")
  )

  val schema1 = StructType(
    List(
      StructField("name", StringType, true),
      StructField("weight", DoubleType, true),
      StructField("animal_type", StringType, true),
      StructField("food" ,StringType, true)
    )
  )

  val df1 = spark.createDataFrame(
    spark.sparkContext.parallelize(data),
    schema1
  )

  df1.columns

  val cols = customerDF.columns.tail

  val result = customerDF
    .select('cid,
      array(cols.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")

  result.show(false)


  result.show(false)

  result.printSchema()

  val iterator = customerDF.schema.productIterator



}
