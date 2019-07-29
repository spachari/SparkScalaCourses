package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class CustomReadInStructure {

  import org.apache.spark.sql.types._

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()
  val customSchema = StructType(Array(
    StructField("guid", StringType, true),
    StructField("hotelid", StringType, true),
    StructField("timestamp", LongType, true),
    StructField("book_time", LongType, true),
    StructField("click_bool", BooleanType, true),
    StructField("book_bool", BooleanType, true),
    StructField("click_time", LongType, true),
    StructField("name", StringType, true),
    StructField("rank", IntegerType, true),
    StructField("travelad", BooleanType, true),
    StructField("dod", BooleanType, true),
    StructField("platform_id", StringType, true),
    StructField("filter_min_price_bool", BooleanType, true),
    StructField("srch_destination_id", StringType, true),
    StructField("filter_themes_bool", BooleanType, true),
    StructField("filter_max_price_bool", BooleanType, true),
    StructField("filter_amenities_bool", BooleanType, true),
    StructField("filter_hotelname", StringType, true),
    StructField("srch_co", StringType, true),
    StructField("filter_min_star_rating_bool", BooleanType, true),
    StructField("tpid", IntegerType, true),
    StructField("sort_type", StringType, true),
    StructField("site_name", StringType, true),
    StructField("srch_id", StringType, true),
    StructField("srch_children_cnt", IntegerType, true),
    StructField("mobile_bool", BooleanType, true),
    StructField("srch_los", IntegerType, true),
    StructField("filter_max_star_rating_bool", BooleanType, true),
    StructField("search_pinned_bool", BooleanType, true),
    StructField("filter_accomodation_types_bool", BooleanType, true),
    StructField("srch_destination_name", StringType, true),
    StructField("srch_adults_cnt", IntegerType, true),
    StructField("srch_bw", IntegerType, true),
    StructField("filter_neighborhoods_bool", BooleanType, true)
  ))


  val csv_df = spark.read.schema(customSchema).parquet("s3://hcom-data-prod-users/spachari/accelerator/file_load/gmt_date=*")
  csv_df.printSchema

}
