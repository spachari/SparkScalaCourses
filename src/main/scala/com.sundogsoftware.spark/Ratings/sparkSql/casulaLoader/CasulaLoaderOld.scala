package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader

import java.time.LocalDateTime

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.{Attribute, Attribute_Metadata}
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.exceptions.{CannotLoadInactiveAttributeException, DataTypeMismatchException, SameNameUsedByDifferentTeamException}
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.utils.LoaderUtils
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.web.EndPointConnector
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CasulaLoaderOld extends App {

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
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._



  val df = Seq(("emailF", "Local", 10, true, "more"),
    ("emailS", "Local", 7, false, "frequent"),
    ("emailR", "International", 5, true,"more"),
    ("emailT", "Local", 7, true, "less"))
    .toDF("trvl_acc_email_addr", "customer_address_type", "customer_rating", "active_customer","frequency_of_visit")

  df.show(false)

  val casulaDataFrame = df
    .select(col(emailColumnName).as("trvl_acc_email_addr"),
      array(attributeColumnsToLoad.map(c => struct(lit(c) as "attribute_name", col(c) cast "string" as "attribute_value")) : _*) as "attributes")
    .withColumn("load_timestamp", current_timestamp())


  casulaDataFrame.show(false)

  val filteredCasulaDataFrame = if (partitions.size == 0)
    casulaDataFrame
  else casulaDataFrame.filter(col(partitionColumnName).isin(partitions))

  filteredCasulaDataFrame.show(false)

  val endPoint = new EndPointConnector("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/")


  //get attributes from MetaData
  println("Calling the endpoint for attributes")
  val attributesInMetadata = endPoint.getMultipleAttributes(attributeColumnsToLoad)

  attributesInMetadata.foreach(println)
  println("attributesInMetadata ... " + attributesInMetadata.length)

  val casulaDFMetaData = for {
    column <- df.limit(1).schema
    if (column.name != emailColumnName)
  }
    yield {Attribute(column.name, LoaderUtils.convertSparkToHiveDataType(column.dataType.toString))}

  println("casula metadata columns ....")
  casulaDFMetaData.foreach(println)
  //Checking if all attributes are active in metadata
  //println("Starting isActive checks .... ")
  if (attributesInMetadata.length > 0) {

    //Check if attributes are active
    try {
      attributesInMetadata.foreach{ case attMetaData =>
        //println(s"Checking ${attMetaData}");
        if (attMetaData.isActive == false) throw CannotLoadInactiveAttributeException(s"${attMetaData.name} is inactive. Please make it active and then try re-loading")}
    }
    catch {
      case x : CannotLoadInactiveAttributeException => x.printMessage(1)
    }

    //Compare attributes against it's metadata ???
    for ( i <- casulaDFMetaData) {
      try {
        val existingAttribute = attributesInMetadata.filter(att => att.name.toLowerCase() == i.attribute_name.toLowerCase())
        existingAttribute.foreach{
          case att => //println(s"Checking for column mismatch for ${att}");
            if (att.dataType.toLowerCase != i.attribute_value.toLowerCase)
              throw DataTypeMismatchException(s"${att.name}'s datatype is ${i.attribute_value} but metadata contains ${att.dataType}. Please correct dataType or change ${att.name} and re-load")}
      } catch {
        case x : DataTypeMismatchException => x.printMessage(2)
      }
    }

    for (i <- casulaDFMetaData) {
      try {
        val existingAttribute = attributesInMetadata.filter(att => att.name.toLowerCase() == i.attribute_name.toLowerCase())
        existingAttribute.foreach{
          case att =>
            if (att.team.toLowerCase != teamName)
              throw SameNameUsedByDifferentTeamException(s"${att.name} is already used by ${att.team}. Please change the column name and re-load")}
        } catch {
        case x : SameNameUsedByDifferentTeamException => x.printMessage(3)
      }
      }
  }


//convertSparkToHiveDataType(
  casulaDFMetaData.foreach(println)

  println("... casulaDFMetaData and attributesInMetadata details ...")
  println(casulaDFMetaData)
  println(attributesInMetadata)

  //Compare activeAttributeList and casulaDataFrameColumnAndTypes data types

  //Create a map of the other data

  val attributesInMetadataMap =  attributesInMetadata.flatMap(x => Map(x.name -> (x))).toMap

  println("Printing map details .... ")
  attributesInMetadataMap.foreach{case x => println(x._1 + " " + x._2)}


  val newAttributesToBeAdded1 = casulaDFMetaData.flatMap{
    case x => println(x.attribute_name + " " + x.attribute_value + " " + attributesInMetadataMap.getOrElse(x.attribute_name, ""));
      if (attributesInMetadataMap.getOrElse(x.attribute_name, "") == "")   Some(x) else None
  }

  println("newAttributesToBeAdded1")
  newAttributesToBeAdded1.foreach(println)
  println("..... newAttributesToBeAdded1")

  val newAttributesMetaData = newAttributesToBeAdded1.map{ case attribute =>
    val attribute_metadata = Attribute_Metadata(
      name = attribute.attribute_name,
      dataType = attribute.attribute_value.toLowerCase,
      team = teamName,
      createdBy = createdBy,
      lastLoadedAt = LocalDateTime.now().toString,
      lastLoadedBy = createdBy,
      isActive = true,
      createdAt = LocalDateTime.now().toString,
      updatedAt =  LocalDateTime.now().toString)
    endPoint.createAttribute(attribute_metadata)}

  //dataHighWayLoader.loadToDataHighWay(filteredCasulaDataFrame)



  val updatedAttributesInMetadata = attributesInMetadata.map{attribute =>
    val updated_attribute = LoaderUtils.updateLastUpdateDateAndUser(attribute, createdBy)
    endPoint.updateAttribute(updated_attribute, updated_attribute.name)}

}
