package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.CasulaLoaderOld.partitionColumnName
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.utils.CheckRuleAndLoadCasula
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CasulaLoader(spark : SparkSession)  {

  def loadTableToCASULA(tableName: String,
                   emailColumnName: String,
                   teamName : String,
                   createdBy : String,
                   paritionColumnName : String = "",
                   partitions: Seq[String] = Seq(),
                   attributeColumnsToLoad: Seq[String] = Seq()): Unit = {

    val casulaDataFrame = spark.table(tableName)
      .select(col(emailColumnName).as("trvl_acc_email_addr"),
        array(attributeColumnsToLoad.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")
      .withColumn("load_timestamp", current_timestamp())


    val filteredCasulaDataFrame = if (partitions.size == 0)
      casulaDataFrame
    else casulaDataFrame.filter(col(partitionColumnName).isin(partitions))

    val checkRuleAndLoadCasula = new CheckRuleAndLoadCasula()

    checkRuleAndLoadCasula.checkMetaDataAndLoad(emailColumnName,
      attributeColumnsToLoad,
      createdBy,
      teamName,
      spark.table(tableName).limit(1),
      filteredCasulaDataFrame)
  }

  def loadDataFrameToCasula(dataFrame: DataFrame,
                            emailColumnName: String,
                            teamName : String,
                            createdBy : String,
                            attributeColumnsToLoad: Seq[String]) = {

    val casulaDataFrame = dataFrame
      .select(col(emailColumnName).as("trvl_acc_email_addr"),
        array(attributeColumnsToLoad.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")
      .withColumn("load_timestamp", current_timestamp())


    val checkRuleAndLoadCasula = new CheckRuleAndLoadCasula()

    checkRuleAndLoadCasula.checkMetaDataAndLoad(emailColumnName,
      attributeColumnsToLoad,
      createdBy,
      teamName,
      dataFrame.limit(1),
      dataFrame : DataFrame)
  }

}
