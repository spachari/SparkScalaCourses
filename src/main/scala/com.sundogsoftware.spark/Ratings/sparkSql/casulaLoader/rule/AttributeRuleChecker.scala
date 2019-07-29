package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.rule

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.{Attribute, Attribute_Metadata}
import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.exceptions.{CannotLoadInactiveAttributeException, DataTypeMismatchException, SameNameUsedByDifferentTeamException}

case class AttributeRuleChecker(attributesInMetadata : List[Attribute_Metadata], casulaDFMetaData : Seq[Attribute], teamName : String) {


  def checkNewAttributeRules(): Unit = {
    checkForInactiveFields()
    checkDataTypeMisMatchInCurrentLoad()
    attributeNameUniqueness(teamName)
  }

  private def checkForInactiveFields() = {
    try {
      attributesInMetadata.foreach{ case attMetaData =>
        if (attMetaData.isActive == false) throw CannotLoadInactiveAttributeException(s"${attMetaData.name} is inactive. Please make it active and then try re-loading")}
    }
    catch {
      case x : CannotLoadInactiveAttributeException => x.printMessage(1)
    }
  }

  private def checkDataTypeMisMatchInCurrentLoad(): Unit = {
    for ( i <- casulaDFMetaData) {
      try {
        val existingAttribute = attributesInMetadata.filter(att => att.name.toLowerCase() == i.attribute_name.toLowerCase())
        existingAttribute.foreach{
          case att =>
            if (att.dataType.toLowerCase != i.attribute_value.toLowerCase)
              throw DataTypeMismatchException(s"${att.name}'s datatype is ${i.attribute_value} but metadata contains ${att.dataType}. Please correct dataType or change ${att.name} and re-load")}
      } catch {
        case x : DataTypeMismatchException => x.printMessage(2)
      }
    }
  }

  private def attributeNameUniqueness(teamName : String) = {
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

}
