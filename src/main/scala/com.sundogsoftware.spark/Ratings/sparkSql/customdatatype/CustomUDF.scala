package com.sundogsoftware.spark.Ratings.sparkSql.customdatatype

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

case class NormalPerson(name: String, age: Int) {
     def aboutMe = s"I am ${name}. I am ${age} years old."
}

case class ReversePerson(name: Int, age: String) {
     def aboutMe = s"I am ${name}. I am ${age} years old."
}



object CustomUDF extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val normalPersons = Seq(NormalPerson("Srinivas", 30),
    NormalPerson("Kirthika", 30),
    NormalPerson("Sadhana", 30),
    NormalPerson("Sadhiv", 30)
  )

  val ds1 = spark.sparkContext.parallelize(normalPersons).toDS

  val ds2 = ds1.map(x => ReversePerson(x.age, x.name))

  ds1.show()

  ds2.show()


}
