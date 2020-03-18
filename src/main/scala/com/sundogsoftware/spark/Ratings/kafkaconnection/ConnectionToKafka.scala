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

  val streamingInputDF = spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers","127.0.0.1:9092")
    .option("subscribe", "first_topic")
    .load()

  //streamingInputDF.printSchema()

  //streamingInputDF.show()

  //--broker-list 127.0.0.1:9092 --topic first_topic


  val query = streamingInputDF
    .writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode(OutputMode.Update())
    .start().awaitTermination()

  // Keep going until we're stopped.

  spark.streams.awaitAnyTermination()

}
