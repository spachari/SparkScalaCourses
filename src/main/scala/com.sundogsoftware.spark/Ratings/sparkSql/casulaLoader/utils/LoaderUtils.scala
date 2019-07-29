package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.utils

import java.time.LocalDateTime

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.Attribute_Metadata
import org.apache.spark.sql.SparkSession

object LoaderUtils {
  def convertSparkToHiveDataType( dataType : String) = {
    dataType.toLowerCase match {
      case "stringtype" => "String"
      case "integertype" => "Int"
      case "datetype" => "Date"
      case "timestamptype" => "TimeStamp"
      case "doubletype" => "double"
      case "booleantype" => "boolean"
    }
  }

  def updateLastUpdateDateAndUser(attribute : Attribute_Metadata, createdBy : String) : Attribute_Metadata = {
    attribute.copy(lastLoadedAt = LocalDateTime.now().toString, lastLoadedBy = createdBy)
  }

  def getEnvironment(spark : SparkSession) = {
    val hiveMetaStoreKey = spark.conf.getAll.filter(_._1.contains("hive.metastore.uris")).keys.mkString
    val hiveMetaStore = spark.conf.get(hiveMetaStoreKey)
    if (hiveMetaStore.contains("lab")) "lab" else "prod"
  }

}
