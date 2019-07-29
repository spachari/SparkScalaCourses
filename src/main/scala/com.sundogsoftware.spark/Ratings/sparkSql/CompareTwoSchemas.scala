package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CompareTwoSchemas extends App {


  import org.apache.spark.sql.DataFrame
  import org.joda.time.LocalDate

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()
  case class FolderNameAndSchema(folderName : String, schemaMap : Map[String, String])

  val startDate = LocalDate.parse("2017-01-01")
  val folderStructure = "s3://big-data-analytics-prod/ODS/project_ldt_xlr/source/hcom/gmt_date="

  def getYearsFolderStructure(folderStructure : String) = {
    val listofFolders = for (i <- 1 to 2) yield {
      val currentDate = startDate.plusDays(i)
      folderStructure + currentDate
    }
    listofFolders.toSeq
  }

  def getCleanedSchema(df : DataFrame) = {
    val df1 = df.schema.fields.map(x =>   (x.name -> x.dataType.toString)).toMap
    df1
  }

  def readFolderAndPrintSchema(folder : String)  = {
    val report = spark.read.parquet(folder)
    val schemaMap = getCleanedSchema(report)
    FolderNameAndSchema(folder, schemaMap)
  }




  def compareMaps(correctMap : FolderNameAndSchema)(itemToCompare : FolderNameAndSchema) = {
    val output = if (!correctMap.schemaMap.equals(itemToCompare.schemaMap)) {
      for (i <- itemToCompare.schemaMap) yield {
        val dataTypeForOtherMap = correctMap.schemaMap.get(i._1).get
        if (i._2 != dataTypeForOtherMap) {
          println(s"${itemToCompare.folderName} deos not match. ${i._1} is different. map is ${i._2} but other value is ${dataTypeForOtherMap}")
        }
      }
    }
  }


  val correctSchemaMap = readFolderAndPrintSchema("s3://big-data-analytics-prod/ODS/project_ldt_xlr/source/hcom/gmt_date=2018-01-01")

  val compare = compareMaps(correctSchemaMap)(_)

  getYearsFolderStructure(folderStructure).map(x => readFolderAndPrintSchema(x)).map(x => compare(x))

}
