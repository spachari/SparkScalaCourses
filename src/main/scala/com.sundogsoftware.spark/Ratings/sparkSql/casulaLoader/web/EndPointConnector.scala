package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.web

import com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain.Attribute_Metadata
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.native.Serialization
import scalaj.http.{Http, HttpResponse}

import scala.util.Try

class EndPointConnector(url: String) {

  implicit val serialization: Serialization.type =  org.json4s.native.Serialization
  implicit val defaults = DefaultFormats

  def getAttribute(attributeName : String) : Attribute_Metadata = {
    val response = Http(s"$url$attributeName").asString

    parse(response.body).extract[Attribute_Metadata]
  }

  def getAttributes(): List[Attribute_Metadata] = {
    val response = Http(s"$url").asString

    parse(response.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil)
  }

  def getMatchingAttributes(columnValueFilter : Map[String,String]) = {
    val responseAsSeq = Http(s"$url").params(columnValueFilter).asString

    parse(responseAsSeq.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil)
  }

  def getMultipleAttributes(columns : Seq[String]) = {
    val responseAsSeq = Try(Http(s"$url"+"?name="+columns.mkString(",")).asString)
    responseAsSeq match {
      case scala.util.Success(value) => value match {
        case x if (x.code == 404) => List()
        case _ => parse(value.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil)}

      case scala.util.Failure(value) => {
        value.printStackTrace();
        sys.exit(8)
        List()
      }
    }
  }

  private def convertAttributeToJson(attribute : Attribute_Metadata) = {
    val jsonObject = ("name" -> attribute.name) ~
      ("dataType" -> attribute.dataType) ~
      ("team" -> attribute.team) ~
      ("createdBy" -> attribute.createdBy) ~
      ("lastLoadedAt" -> attribute.lastLoadedAt) ~
      ("lastLoadedBy" -> attribute.lastLoadedBy) ~
      ("isActive" -> attribute.isActive)
    compact(render(jsonObject))
  }


  def createAttribute(attribute : Attribute_Metadata) = {
    val stringifiedData = convertAttributeToJson(attribute)
    val response = try {
      Http(s"$url").postData(stringifiedData).asString
    } catch {
      case x : java.net.SocketTimeoutException => x.getMessage
    }

    response match {
      case x : HttpResponse[String] if x.code == 201 => println(s"Attribute $attribute has been successfully created")
      case exception => { println(s"Attribute $attribute could not be created" + exception);  sys.exit(4) }
    }
  }

  def updateAttribute(attribute : Attribute_Metadata, attributeName : String) = {
    val stringifiedData = convertAttributeToJson(attribute)
    val response = try {
      Http(s"$url${attribute.name}").postData(stringifiedData).method("put").asString
    } catch {
      case x : java.net.SocketTimeoutException => x.getMessage
    }

    response match {
      case x : HttpResponse[String] if x.code == 200 => println(s"Attribute $attribute has been successfully updated")
      case exception => { println(s"Attribute $attribute could not be updated"); sys.exit(5) }
    }
  }

  def updateOrCreateAttributes(attributeList : List[Attribute_Metadata] , f : Attribute_Metadata => HttpResponse[String])  = {
    val output = for (attribute <- attributeList) yield {
      val response = f(attribute)
      Option(attribute.name, response.code)
    }
    output
  }

}

object EndPointConnector {
  def apply(url : String) = new EndPointConnector(url)
}