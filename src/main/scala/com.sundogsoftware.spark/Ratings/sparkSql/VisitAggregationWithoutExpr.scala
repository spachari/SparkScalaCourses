package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}

object VisitAggregationWithoutExpr extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .config("spark.sql.warehouse.dir", "file:///C:/temp")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val guidtests = Seq(
    ("GuidA","251463", 1539668181881L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidA","251463", 1539668281882L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidA","251463", 1539668381883L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidB","251463", 1539678481884L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidB","251463", 1539678581885L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidB","251463", 1539678681886L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidC","251463", 1539698781887L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidC","251463", 1539698881888L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345"),
    ("GuidC","251463", 1539698981889L, "mdp.hcom.BR.011.387.02.42","Brand", "mdp.hcom.BR.011.387.02.42","39.722", "-75.5386", "2018-09-20", "2018-09-21", "home page", "12345")
  ).toDF("guid", "clientId", "timestamp", "visitmarketingcode","marketingCode","fullMarketingTrackingCode","latitude","longitude","checkinDateassearch_check_in_date","checkinDateassearch_check_out_date","pagename","purchaseID")


  val guidsWithGmt = guidtests.withColumn("gmt", ($"timestamp" / 1000) cast "Int")

  val output = guidsWithGmt.withColumn("previous_gmt", lag("gmt",1,-1).over(Window.partitionBy("guid").orderBy("timestamp")))

  val outputWithPreviousLag = output.withColumn("previous_lag", $"gmt"-$"previous_gmt")

  val outputWithStartVisit = outputWithPreviousLag.withColumn("start_visit", when(col("previous_lag") > 1800, $"gmt").otherwise(null))

  val outputWithStartVisitNumberGuidOtherWay = outputWithStartVisit.withColumn("visit_number_guid", first($"start_visit", true).over(Window.partitionBy("guid").orderBy("timestamp")))

  val visitDF = outputWithStartVisitNumberGuidOtherWay.withColumn("visit_start_timestamp", min(($"gmt").cast(TimestampType)).over(Window.partitionBy("guid","visit_number_guid"))).drop("gmt","previous_gmt","previous_lag","start_visit", "visit_number_guid")

  visitDF.select($"guid", $"timestamp",$"visit_start_timestamp").show(false)

  object WindowUtils {
    val propertyPageList = List("Mob :: TabWeb :: hotel details page description tab",
      "Mob :: Web :: hotel details page description tab",
      "Mob :: Web :: hotel details without dates description tab",
      "Mob :: aApp :: hotel details page description tab",
      "Mob :: iApp :: hotel details page description tab",
      "Mob :: iApp :: hotel details without dates description tab",
      "hotel details page description tab",
      "hotel details without dates description tab")
  }

  val partitionWindow = Window.partitionBy($"guid",$"visit_start_timestamp").orderBy($"timestamp")
  val partitionWindowRemoveNulls = Window.partitionBy($"guid",$"visit_start_timestamp").orderBy($"timestamp").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)


  val windowDF = visitDF
    .withColumnRenamed("purchaseID", "purchase_id")
    .withColumn("client_id", first("clientId") over (partitionWindow))
    .withColumn("visit_marketing_code", first("visitMarketingCode") over (partitionWindow))
    .withColumn("restricted_last_touch_marketing_code", first("marketingCode") over (partitionWindow))
    .withColumn("last_touch_marketing_code", first("fullMarketingTrackingCode") over (partitionWindow))
    .withColumn("latitude", first("latitude") over (partitionWindow))
    .withColumn("longitude", first("longitude") over (partitionWindow))
    .withColumn("last_searched_check_in_date", last("checkinDateassearch_check_in_date", true) over (partitionWindowRemoveNulls))
    .withColumn("last_searched_check_out_date", last("checkinDateassearch_check_out_date", true) over (partitionWindowRemoveNulls))




  val groupedDF = windowDF
    .groupBy(
      $"guid",
      $"client_id",
      $"visit_marketing_code",
      $"restricted_last_touch_marketing_code",
      $"last_touch_marketing_code",
      $"latitude",
      $"longitude",
      $"last_searched_check_in_date",
      $"last_searched_check_out_date",
      $"pagename",
      $"purchase_id"
    )
    .agg(
      min("timestamp").alias("visit_start_timestamp_gmt"),
      max("timestamp").alias("visit_end_timestamp_gmt"),
      count("guid").alias("hits_in_visit"),
      sum(when(col("purchase_id").isNotNull, 1).otherwise(0)).alias("visit_converted_counts"),
      sum(when(col("pagename").isin(WindowUtils.propertyPageList: _*), 1).otherwise(0)).alias("no_property_pages_viewed")
    )

  val finalDF = groupedDF
    .select(
      $"guid",
      $"client_id",
      $"visit_start_timestamp_gmt".alias("min_timestamp"),
      $"visit_end_timestamp_gmt".alias("max_timestamp"),
      $"hits_in_visit",
      $"visit_marketing_code",
      $"restricted_last_touch_marketing_code",
      $"last_touch_marketing_code",
      $"latitude".cast(DoubleType).alias("latitude"),
      $"longitude".cast(DoubleType).alias("longitude"),
      $"last_searched_check_in_date",
      $"last_searched_check_out_date",
      $"no_property_pages_viewed",
      $"visit_converted_counts"
    )
    .withColumn("visit_converted", when($"visit_converted_counts" === 0, false).otherwise(true))
    .drop("visit_converted_counts")

  finalDF.show(false)

}
