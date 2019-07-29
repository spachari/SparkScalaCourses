package com.sundogsoftware.spark.Ratings

import org.apache.spark.sql.SparkSession

object test extends App  {

  val spark = SparkSession
    .builder
    .appName("test")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .getOrCreate()

}
