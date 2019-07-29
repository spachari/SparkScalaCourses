package com.sundogsoftware.spark.Ratings

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object testDataFrameToRDD extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession
    .builder
    .appName("test")
    .master("local[5]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .getOrCreate()


case class Person(age: Long, name: String) extends Serializable

  import spark.implicits._

val df = Seq(Person(24, "pedro"), Person(22, "fritz")).toDF()

val myWholeRDD : RDD[Row] = df.select("*").rdd

  myWholeRDD.foreach(println)




def convertRowToPerson (row : Row) : Person = {
  val output = row.toString().split(",")
  val age = row.getLong(0)
  val name = row.get(1).toString
  val person = Person(age,name)
  person
}

  //Convert the rdd to a pojo
  val myWholeRDDAsPerson = myWholeRDD.map(x => convertRowToPerson(x))



  //Use your function after you have converted the pojo
  myWholeRDDAsPerson.foreach(x => println(x.name + " -- " + x.age))

}
