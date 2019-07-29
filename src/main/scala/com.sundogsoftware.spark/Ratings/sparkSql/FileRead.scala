package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.joda.time.LocalDate

import scala.util.{Failure, Success, Try}

object FileRead {


  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()

  val startDate = LocalDate.parse("2018-07-04")
  val folderStructure = "s3://big-data-analytics-prod/ODS/project_ldt_xlr/source/hcom/gmt_date="

  def getYearsFolderStructure(folderStructure : String) = {
    val listofFolders = for (i <- 1 to 48) yield {
      val currentDate = startDate.plusDays(i)
      folderStructure + currentDate
    }
    listofFolders.toSeq
  }

  val allFolders = getYearsFolderStructure(folderStructure)

  def readFile(folder : String) = {
    val report = spark.read.parquet(folder)
    println(report.count())
  }

  for (folder <- allFolders) {
    val fileRead = Try(spark.read.parquet(folder))

    fileRead match {
      case Success(file) => {
        println("successfull read")
      }
      case Failure(file) => {
        spark.emptyDataFrame.write.mode(SaveMode.Overwrite).parquet(folder)
        println(folder + " empty df created")
      }
    }
    readFile(folder)
  }

}
