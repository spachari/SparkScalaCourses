package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

case class Persons1(name: String, age: Int, personid : Int)
case class Profile(name: String, personid : Int , profileDescription: String)

object CreatingASQLContextAndJoiningTwoDataFrames extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkSessionZipsExample")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val df1 = spark.sqlContext.createDataFrame(
      Persons1("Bindu",20,  2) ::
      Persons1("Raphel",25, 5) ::
      Persons1("Ram",40, 9):: Nil)

  val df2 = spark.sqlContext.createDataFrame(
    Profile("Spark",2,  "SparkSQLMaster")
      :: Profile("Spark",5, "SparkGuru")
      :: Profile("Spark",9, "DevHunter"):: Nil
  )

  val df_asPerson = df1.as("dfPerson")
  val df_asProfile = df2.as("dfProfile").persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  val joinedDF = df_asPerson.join(
    df_asProfile
      .select(col("personid"),col("name"), col("profileDescription"))
      .where("name == 'Spark'"),
  col("dfPerson.personid") === col("dfProfile.personid"))

  val output = joinedDF.select(col("dfPerson.personid"),
                  col("dfPerson.name"),
                  col("age"),
                  col("profileDescription")
  )

  output.registerTempTable("temp_table")
  output.show(10)


}
