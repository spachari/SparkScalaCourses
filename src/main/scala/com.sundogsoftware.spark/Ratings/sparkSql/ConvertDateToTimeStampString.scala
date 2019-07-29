package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._


object ConvertDateToTimeStampString extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._
  case class PersonWithDOB (name : String, dob : String)
  val sampleDF = List(
    PersonWithDOB("Srinivas","2018-08-01"),
    PersonWithDOB("Kirthika","2018-01-01")
  ).toDF("name", "dob")


  val convertedDF = sampleDF.select($"name", $"dob".cast(DateType))

  val convertDateToString = convertedDF.select($"name", $"dob")
    .withColumn("dobasstring", concat(substring(col("dob"),1,4),
                                                substring(col("dob"),6,2),
                                                substring(col("dob"),9,2),lit("T000000Z") ))

  convertDateToString.show()

  //20190113T064015Z
  //20180801T000000Z

  sampleDF.select(col("name")).rdd.foreach(println)

  println("Printing the dataType")
  sampleDF.select(col("name")).rdd.collect().map(_.schema.fields.head.dataType)


  println("Done")
  sampleDF.select(col("name")).rdd.map(_.schema.fields).foreach{ case x => println(x)}

}
