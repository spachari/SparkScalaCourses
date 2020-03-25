package com.sundogsoftware.spark.Ratings.kafkaconnection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object ConnectionToKafka extends App {

  val spark = SparkSession
    .builder
    .appName("StructuredWatermarkingWC")
    .master("local[*]")
    .config("spark.sql.treaming.chcekpointing","/tmp/")
    .getOrCreate()
  spark.sparkContext.setLogLevel("DEBUG")

  val streamingInputDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","127.0.0.1:9092")
    .option("subscribe", "streaming_test")
    .load()



  val query = streamingInputDF
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start().awaitTermination()

  spark.streams.awaitAnyTermination()

}
