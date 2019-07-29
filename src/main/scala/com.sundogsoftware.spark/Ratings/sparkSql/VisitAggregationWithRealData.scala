package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType

object VisitAggregationWithRealData extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  //Create a spark session with hive enabled
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("StringToDateConverter")
    .enableHiveSupport()
    .getOrCreate()


  /*
  +-------------------+------------------------------------+-----------+------------------+-------------+-------------------------+--------+---------+---------+--------+---------+-----------------------------------------------------+----------------------------------------------------------+-----------+------------+------------------+-------------+---------------------+----------------+
    |guidHash           |guid                                |platformid |visitmarketingcode|marketingCode|fullMarketingTrackingCode|latitude|longitude|visitorId|clientId|dossierId|email                                                |pagename                                                  |checkinDate|checkoutDate|omniturePurchaseID|timestamp    |visit_start_timestamp|visit_start_date|
  +-------------------+------------------------------------+-----------+------------------+-------------+-------------------------+--------+---------+---------+--------+---------+-----------------------------------------------------+----------------------------------------------------------+-----------+------------+------------------+-------------+---------------------+----------------+
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264350732|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264352054|2017-01-01 09:52:30  |2017-01-01      |

    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264352374|2017-01-01 09:52:30  |2017-01-01      |

    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: Sign In Submit - success                   |null       |null        |null              |1483264353720|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: Welcome Rewards Activity                   |null       |null        |null              |1483264354748|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264355139|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: Customer Service :: Loyalty Landing        |null       |null        |null              |1483264355916|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264384621|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264385000|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264385422|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: Sign In Submit - success                   |null       |null        |null              |1483264386972|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |null    |null     |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264388304|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |null       |null        |null              |1483264408111|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: search result with dates                   |2017-03-05 |2017-03-06  |null              |1483264416889|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |2017-03-05 |2017-03-06  |null              |1483264420321|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: hotel details without dates description tab|2017-03-05 |2017-03-06  |null              |1483264446391|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp ::                                            |2017-03-05 |2017-03-06  |null              |1483264447331|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: hotel details page description tab         |2017-03-05 |2017-03-06  |null              |1483264447342|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: hotel details without dates description tab|2017-03-05 |2017-03-06  |null              |1483264464384|2017-01-01 09:52:30  |2017-01-01      |
    |5281058263413808215|034AEF5D-999F-48BD-B0C1-E1558E01DFEE|Mob :: iApp|Mob :: iApp       |Mob :: iApp  |null                     |22.35   |114.13   |null     |389100  |62131611 |$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng|Mob :: iApp :: hotel details page description tab         |2017-03-05 |2017-03-06  |null              |1483264465357|2017-01-01 09:52:30  |2017-01-01      |

*/



  import spark.implicits._


  /*
  val guidtests = Seq(
    ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", 1483264350732L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[Double], null.asInstanceOf[Double], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01")
  ).toDF("guid", "clientId", "timestamp", "visitmarketingcode","marketingCode","fullMarketingTrackingCode","latitude","longitude","checkinDateassearch_check_in_date","checkinDateassearch_check_out_date","pagename","purchaseID","visit_start_timestamp","visit_start_date")
*/

  val visitDF = Seq(
    ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264350732L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264352054L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264352374L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264353720L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp :: Sign In Submit - success", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264354748L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp :: Welcome Rewards Activity", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264355139L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264355916L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp :: Customer Service :: Loyalty Landing", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264384621L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264385000L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264385422L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264386972L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp :: Sign In Submit - success", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264388304L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264408111L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264416889L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp :: search result with dates", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264420321L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], null.asInstanceOf[String], "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264446391L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], "22.35", "114.13", "2017-03-05", "2017-03-06", "Mob :: iApp :: hotel details without dates description tab", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264447331L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], "22.35", "114.13", "2017-03-05", "2017-03-06", "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264447342L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], "22.35", "114.13", "2017-03-05", "2017-03-06", "Mob :: iApp :: hotel details page description tab", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264464384L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], "22.35", "114.13", "2017-03-05", "2017-03-06", "Mob :: iApp ::", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01"),
      ("034AEF5D-999F-48BD-B0C1-E1558E01DFEE","389100", "$parallax$UjJmfOshB9HpthwfFUfsBIMsrh348Ki9HdnXbK/Rrng",1483264465357L, "Mob :: iApp","Mob :: iApp", null.asInstanceOf[String], "22.35", "114.13", "2017-03-05", "2017-03-06", "Mob :: iApp :: hotel details page description tab", null.asInstanceOf[String], "2017-01-01 09:52:30","2017-01-01")
  ).toDF("guid", "clientId", "email","timestamp", "visitmarketingcode","marketingCode","fullMarketingTrackingCode","latitude","longitude","checkinDate","checkoutDate","pagename","omniturePurchaseID","visit_start_timestamp","visit_start_date")

  visitDF.show()


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
    .withColumn("last_searched_check_in_date", last("checkinDate", true) over (partitionWindowRemoveNulls))
    .withColumn("last_searched_check_out_date", last("checkoutDate", true) over (partitionWindowRemoveNulls))


  println("printing windowDF")
  windowDF.where($"guid" === "034AEF5D-999F-48BD-B0C1-E1558E01DFEE").show(false)

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
      $"visit_start_date"
    )
    .agg(
      min("timestamp").alias("visit_start_timestamp_gmt"),
      max("timestamp").alias("visit_end_timestamp_gmt"),
      count("guid").alias("hits_in_visit"),
      sum(when(col("omniturePurchaseID").isNotNull, 1).otherwise(0)).alias("visit_converted_counts"),
      sum(when(col("pagename").isin(WindowUtils.propertyPageList: _*), 1).otherwise(0)).alias("no_property_pages_viewed")
    )

  println("printing groupedDF")
  groupedDF.where($"guid" === "034AEF5D-999F-48BD-B0C1-E1558E01DFEE").show(false)

  val visitStagingDF = groupedDF
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
      $"visit_converted_counts",
      $"visit_start_date".as("visit_date")
    )
    .withColumn("visit_converted", when($"visit_converted_counts" === 0, false).otherwise(true))
    .drop("visit_converted_counts")


  println("printing visitStagingDF")
  visitStagingDF.where($"guid" === "034AEF5D-999F-48BD-B0C1-E1558E01DFEE").show(false)


  /*
  val testDF = Seq(
    ("Srinivas"),
    (null.asInstanceOf[String])
  ).toDF("name")

  testDF.show()
  */
}
