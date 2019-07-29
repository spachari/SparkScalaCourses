package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.test

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.CasulaLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CasulaLoaderTest extends App {

  val tableName = "bix_staging.casulatest"
  val emailColumnName = "trvl_acc_email_addr"
  val teamName = "hde cas"
  val createdBy = "Srinivas"
  val partitionColumnName = ""
  val  partitions = Seq()
  val attributeColumnsToLoad = Seq("customer_address_type", "customer_rating", "active_customer","frequency_of_visit")

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("partitionTutorial")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .config("spark.hadoop.hive.metastore.uris","thrift://metastore-proxy.waggledance-us-west-2.hcom-data-lab.aws.hcom:48869")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._



  val df = Seq(("emailF", "Local", 10, true, "more"),
    ("emailS", "Local", 7, false, "frequent"),
    ("emailR", "International", 5, true,"more"),
    ("emailT", "Local", 7, true, "less"))
    .toDF("trvl_acc_email_addr", "customer_address_type", "customer_rating", "active_customer","frequency_of_visit")

  df.show(false)

  val casulaLoader = new CasulaLoader(spark)

  casulaLoader.loadDataFrameToCasula(
    df,
    "trvl_acc_email_addr",
    teamName,
    createdBy,
    attributeColumnsToLoad)

}
