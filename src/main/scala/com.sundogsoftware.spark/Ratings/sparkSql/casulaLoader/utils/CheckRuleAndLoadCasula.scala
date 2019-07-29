package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.utils

import java.time.LocalDateTime

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.{Attribute, Attribute_Metadata}
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.rule.AttributeRuleChecker
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.web.EndPointConnector
import org.apache.spark.sql.DataFrame

class CheckRuleAndLoadCasula {

    def checkMetaDataAndLoad(emailColumnName : String,
                             attributeColumnsToLoad: Seq[String],
                             createdBy : String,
                             teamName : String,
                             dataFrameSchema : DataFrame,
                             filteredCasulaDataFrame : DataFrame
                            ) = {
      val endPoint = new EndPointConnector("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/")

      val attributesInMetadata = endPoint.getMultipleAttributes(attributeColumnsToLoad)

      val casulaDFMetaData = for {
        column <- dataFrameSchema.schema
        if (column.name != emailColumnName)
      }
        yield {Attribute(column.name, LoaderUtils.convertSparkToHiveDataType(column.dataType.toString))}

      casulaDFMetaData.foreach(println)

      if (attributesInMetadata.length > 0) {
        val attributeRuleChecker = AttributeRuleChecker(attributesInMetadata, casulaDFMetaData, teamName)
        attributeRuleChecker.checkNewAttributeRules()
      }

      val attributesInMetadataMap =  attributesInMetadata.flatMap(x => Map(x.name -> (x))).toMap

      val newAttributesToBeAdded1 = casulaDFMetaData.flatMap{
        case x => println(x.attribute_name + " " + x.attribute_value + " " + attributesInMetadataMap.getOrElse(x.attribute_name, ""));
          if (attributesInMetadataMap.getOrElse(x.attribute_name, "") == "")   Some(x) else None
      }

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


      //Load new attributes
      newAttributesMetaData.map{attribute => endPoint.createAttribute(attribute)}

      //dataHighWayLoader.loadToDataHighWay(filteredCasulaDataFrame)

      val updatedAttributesInMetadata = attributesInMetadata.map(x => LoaderUtils.updateLastUpdateDateAndUser(x, createdBy))

      updatedAttributesInMetadata.map{attribute => endPoint.updateAttribute(attribute, attribute.name)}
    }

}
