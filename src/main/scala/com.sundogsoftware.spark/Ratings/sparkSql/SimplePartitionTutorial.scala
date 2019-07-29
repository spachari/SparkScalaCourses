package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object SimplePartitionTutorial extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("partitionTutorial")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()


  import spark.implicits._

  val x = (1 to 10).toList
  val numbersDF = x.toDF("numbers")

  println("Default number of partitions numbersDF " + numbersDF.rdd.partitions.size)

  numbersDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/csv-output/")

  //Let's reduce the number of partitions
  val repartitionDF = numbersDF.repartition(2)

  numbersDF.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

  println("Number of partitions after numbersDF repartition(2) " + numbersDF.rdd.partitions.size)

  val coalesceDF = numbersDF.coalesce(2)

  println("Number of partitions after numbersDF coalesce(2) " + coalesceDF.rdd.partitions.size)

  repartitionDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/csv-output-partitioned/")

  //There is not much difference between repartition and coalesce as they both are needed to configure partitions
  case class People(name: String, age : Int, favColour : String, sex : String)

  val peopleList = List(
    People("Srinivas", 37, "blue","M"),
    People("Kirthika", 37, "pink","F"),
    People("Sadhana", 6, "blue","F"),
    People("Sachin", 30, "blue","M"),
    People("Sadhiv", 1, "black","M"),
    People("Kamila", 20, "rose","F")
  )

  val peopleDF = peopleList.toDF("name","age","colour","sex")

  peopleDF.select("name")


  println("Default number of partitions peopleDF " + peopleDF.rdd.partitions.size)
  peopleDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/peopleDF/")

  val repartitionOnColorDF = peopleDF.repartition($"colour")
  println("partitioned on color " + repartitionOnColorDF.rdd.partitions.size)
  repartitionOnColorDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionedPeopleDF/")

  val repartitionOnColorANdFourDF = peopleDF.repartition($"colour")
  println("partitioned on color and 4 " + repartitionOnColorANdFourDF.rdd.partitions.size)
  repartitionOnColorANdFourDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionOnColorAndFourDF/")

  val repartitionOnColorAndSexDF = peopleDF.repartition($"colour",$"sex")
  println("partitioned on color and sex " + repartitionOnColorAndSexDF.rdd.partitions.size)
  repartitionOnColorAndSexDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionOnColorAndSexDF/")

  val repartitionOnColorSex10DF = peopleDF.repartition($"colour",$"sex").repartition(10)
  println("partitioned on color and sex " + repartitionOnColorSex10DF.rdd.partitions.size)
  repartitionOnColorSex10DF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionOnColorSex10DF/")

  val repartitionOnColorSex10inOneDF = peopleDF.repartition(10, $"colour",$"sex")
  println("partitioned on color and sex " + repartitionOnColorSex10DF.rdd.partitions.size)
  repartitionOnColorSex10DF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionOnColorSex10DF/")

  case class Colour(colour : String)
  case class Sex(sex : String)

    val rangePartitioner = new org.apache.spark.RangePartitioner(2, peopleDF.rdd.keyBy(x => x.getString(0)).mapValues(x => x.getInt(1)))
    val repartitionOnrepartitionAndSortWithinPartitions = peopleDF.rdd.keyBy(x => x.getString(0)).mapValues(x => x.getInt(1))
      .repartitionAndSortWithinPartitions(rangePartitioner)

  println("partitioned on color and sex " + repartitionOnrepartitionAndSortWithinPartitions.partitions.size)
  repartitionOnrepartitionAndSortWithinPartitions.toDF.write.mode(SaveMode.Overwrite)csv("/Users/spachari/Desktop/Spark-learning/repartitionOnColorSex10DF/")
  repartitionOnrepartitionAndSortWithinPartitions.foreach(println)


  peopleDF.show()

  peopleDF.where($"sex" === "M").agg(max(col("age"))).show()
}
