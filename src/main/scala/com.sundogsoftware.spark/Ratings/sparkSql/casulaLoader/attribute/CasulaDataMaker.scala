package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.attribute

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.CasulaLoaderOld.{attributeColumnsToLoad, emailColumnName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CasulaDataMaker(attribute : String)(implicit val spark : SparkSession) {

  def makeCasulaDataFrameFromDF(df : DataFrame) = {
    df
      .select(col(emailColumnName).as("trvl_acc_email_addr"),
        array(attributeColumnsToLoad.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")
      .withColumn("load_timestamp", current_timestamp())

  }

  def makeCasulaDataFrameFromTable(table : String) = {
    spark.table(table)
      .select(col(emailColumnName).as("trvl_acc_email_addr"),
        array(attributeColumnsToLoad.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")
      .withColumn("load_timestamp", current_timestamp())
  }

}
