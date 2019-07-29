package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.attribute
import java.time.LocalDateTime

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.{Attribute, Attribute_Metadata}
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.web.EndPointConnector

class AttributeLoader(casulaDFMetaData : Seq[Attribute],
                      attributesInMetadata : Seq[Attribute_Metadata],
                      teamName : String,
                      createdBy : String,
                      endPoint : EndPointConnector) {

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

  val newAttributesMetaData = newAttributesToBeAdded1.map{ case attribute => Attribute_Metadata(
    name = attribute.attribute_name,
    dataType = attribute.attribute_value.toLowerCase,
    team = teamName,
    createdBy = createdBy,
    lastLoadedAt = LocalDateTime.now().toString,
    lastLoadedBy = createdBy,
    isActive = true,
    createdAt = LocalDateTime.now().toString,
    updatedAt =  LocalDateTime.now().toString)}

  println("new attribute list ...")
  newAttributesMetaData.foreach(println)
  println("..................")

  //Load new attributes
  newAttributesMetaData.map{attribute => endPoint.createAttribute(attribute)}

  //dataHighWayLoader.loadToDataHighWay(filteredCasulaDataFrame)

  def updateLastUpdateDateAndUser(attribute : Attribute_Metadata) : Attribute_Metadata = {
    attribute.copy(lastLoadedAt = LocalDateTime.now().toString, lastLoadedBy = createdBy)
  }

  val updatedAttributesInMetadata = attributesInMetadata.map(x => updateLastUpdateDateAndUser(x))

  println("updated attribute list ... ")
  updatedAttributesInMetadata.foreach(println)

  updatedAttributesInMetadata.map{attribute => endPoint.updateAttribute(attribute, attribute.name)}
}
