package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.StringDecoder


object SimpleKafkaConnection {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[8]").setAppName("SimplekafkaConnect")

    val ssc = new StreamingContext(conf, Seconds(5))



    val kafkaTopicRaw = "test"
    val kafkaBroker = "localhost:9092"


    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)


    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    kafkaStream.foreachRDD(rdd => rdd.foreach(x => println(x._1 + " -- " + x._2)))

    ssc.start()
    ssc.awaitTermination()


  }


}
