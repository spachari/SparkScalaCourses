package com.sundogsoftware.spark.Ratings

import java.sql.{Date, Timestamp}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GetColumnNameAndDataType extends App {

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
    ("C1", "Jackie Chan", 50, "Dayton", "M", Date.valueOf("2016-09-30"), Timestamp.valueOf("2016-09-30 01:01:30") ,98.0, true),
    ("C2", "Harry Smith", 30, "Beavercreek", "M",Date.valueOf("2016-09-30"),Timestamp.valueOf("2016-09-30 01:01:30") , 10.0,true),
    ("C3", "Ellen Smith", 28, "Beavercreek", "F",Date.valueOf("2016-09-30"),Timestamp.valueOf("2016-09-30 01:01:30") , 12.0,true),
    ("C4", "John Chan", 26, "Dayton","M", Date.valueOf("2016-09-30"),Timestamp.valueOf("2016-09-30 01:01:30") ,12.2,true)
  ).toDF("cid","name","age","city","sex","dates" ,"times","double","bool")

  customerDF.schema.foreach{ case x => println(x.name, x.dataType) }

  val dataFrames = for (i <- customerDF.schema) yield {(i.name, i.dataType)}

}
