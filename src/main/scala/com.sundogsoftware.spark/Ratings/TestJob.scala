package com.sundogsoftware.spark.Ratings

import org.apache.spark.{SparkConf, SparkContext}

object TestJob extends App {

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("Word Count")
  val sc = new SparkContext(conf)

  //val ss2 = Seq(message,message1,message2)

  //val rdd = sc.parallelize(ss2)

  //val ss2 = Seq(message,message1,message2)

  val rdd = sc.parallelize(Seq(1,2,3))

  println(rdd.count())
  println("entering datahighway code again")


}
