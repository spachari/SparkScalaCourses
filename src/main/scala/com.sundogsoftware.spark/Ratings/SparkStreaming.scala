package com.sundogsoftware.spark.Ratings

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkStreaming extends App {


  val sc = new SparkContext()


  val myRDD = sc.parallelize(Seq(1,2,3))

  val spark = SparkSession.builder().appName("test").master("local[1]")




  //spark.getOrCreate().createDataFrame(myRDD, )

}
