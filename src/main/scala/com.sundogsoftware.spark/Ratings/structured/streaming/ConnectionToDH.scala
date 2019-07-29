package com.sundogsoftware.spark.Ratings.structured.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object ConnectionToDH extends App {


  val spark = SparkSession
    .builder
    .appName("StructuredWatermarkingWC")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val clickStream = spark
    .readStream
    .format("data-highway")
    .option("data-highway-host", "datahighway.us-west-2.hcom-data-prod.aws.hcom")
    .option("road-name", "hcom_clickstream")
    .option("stream-name", "hcom_clickstream_visits")
    .option("micro-batch-duration", 5000)
    .option("partitions-per-batch", 10)
    .option("include-partitions-and-offset-column", true)
    .load()

  clickStream.writeStream
    .outputMode(OutputMode.Update) // watermarking demands Append- or Update-mode
    .format("console")
    .option("truncate", false)
    .start()

}
