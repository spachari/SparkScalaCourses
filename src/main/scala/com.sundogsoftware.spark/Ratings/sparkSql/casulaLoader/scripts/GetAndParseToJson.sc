

import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import scalaj.http._

import scala.util.Try

case class Attribute_Metadata(
                             name : String,
                             `type` : String,
                             team : String,
                             createdBy : String,
                             lastLoadedAt : String,
                             lastLoadedBy : String,
                             isActive : Boolean,
                             createdAt : String,
                             updatedAt : String
                             )



implicit val serialization: Serialization.type =  org.json4s.native.Serialization

// the `query` parameter is automatically url-encoded
// `sort` is removed, as the value is not defined

val response = Http("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/test_attribute_1").asString

implicit val defaults = DefaultFormats

// response.unsafeBody: by default read into a String
println("Response is " + parse(response.body).extract[Attribute_Metadata])

println("DONE")

val responses = Http("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/").asString

println("Response is " + parse(responses.body).children.map(x => x.children.map(x => x)))

parse(responses.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil).foreach(println)


//.params(Map("isActive" -> "true")


val responseAsSeq = Http("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/").params(Map("isActive" -> "true", "createdBy" -> "Srini")).asString




println(responseAsSeq)

parse(responseAsSeq.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil).foreach(println)


def getMultipleAttributes(url : String, columns : Seq[String]) = {
  val responseAsSeq = Try(Http(s"$url"+"?name="+columns.mkString(",")).asString)
  println(responseAsSeq)
  responseAsSeq match {
    case scala.util.Success(value) => parse(value.body).extract[Map[String, List[Attribute_Metadata]]].getOrElse("result", Nil)
    case scala.util.Failure(value) => List()
  }
}

val output1 = getMultipleAttributes("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/", Seq("test_attribute_1"))


println(output1)

val output = getMultipleAttributes("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/", Seq("test_attribute_1000"))

println(output)

