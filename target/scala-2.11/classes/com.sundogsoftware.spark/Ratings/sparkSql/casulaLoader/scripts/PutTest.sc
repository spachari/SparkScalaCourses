import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scalaj.http._


//case class Attribute_Metadata(
//                               name : String,
//                               `type` : String,
//                               team : String,
//                               createdBy : String,
//                               lastLoadedAt : String,
//                               lastLoadedBy : String,
//                               isActive : Boolean
//                             )

case class Attribute_Metadata(
                               name : String,
                               dataType : String,
                               team : String,
                               createdBy : String,
                               lastLoadedAt : String,
                               lastLoadedBy : String,
                               isActive : Boolean,
                               createdAt : String,
                               updatedAt : String
                             )


val newAttribute = new Attribute_Metadata("test_attribute_3",
  "boolean","HDE CAS","bob","2019-05-01T08:00:00.000Z",
  "Srini Vedant", true,"","")



//val json = ("name" -> newAttribute.name) ~
//  ("type" -> newAttribute.`type`) ~
//  ("team" -> newAttribute.team) ~
//  ("createdBy" -> newAttribute.createdBy) ~
//  ("lastLoadedAt" -> newAttribute.lastLoadedAt) ~
//  ("lastLoadedBy" -> newAttribute.lastLoadedBy) ~
//  ("isActive" -> newAttribute.isActive)
//
//val data = compact(render(json))
//
//val response = Http("https://752xdmq5y8.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/").postData(data).asString
//
//print(response.body)

val test_attribute_8 = new Attribute_Metadata("test_attribute_9",
  "boolean","HDE CAS","Srini","2019-05-03T11:00:00.000Z",
  "Vedant", true,"","")
val test_attribute_9 = new Attribute_Metadata("test_attribute_10",
  "boolean","HDE CAS","Srini","2019-05-03T11:00:00.000Z",
  "Vedant", true,"","")
val test_attribute_10 = new Attribute_Metadata("test_attribute_14",
  "boolean","HDE CAS","Srini","2019-05-03T11:00:00.000Z",
  "Srinivas", false,"2019-05-21T10:15:16.000Z","2019-05-21T10:15:16.000Z")
val test_attribute_11 = new Attribute_Metadata("test_attribute_15",
  "boolean","HDE CAS","Srini","2019-05-03T11:00:00.000Z",
  "Srinivas", false,"2019-05-21T10:15:16.000Z","2019-05-21T10:15:16.000Z")


val attributeList = List(test_attribute_8, test_attribute_9, test_attribute_10, test_attribute_11)

def convertAttributeToJson(attribute : Attribute_Metadata) = {
  ("name" -> attribute.name) ~
    ("dataType" -> attribute.dataType) ~
    ("team" -> attribute.team) ~
    ("createdBy" -> attribute.createdBy) ~
    ("lastLoadedAt" -> attribute.lastLoadedAt) ~
    ("lastLoadedBy" -> attribute.lastLoadedBy) ~
    ("isActive" -> attribute.isActive)
}


def postAttribute(attribute : Attribute_Metadata) = {
  val jsonObject = convertAttributeToJson(attribute)
  val stringifiedData = compact(render(jsonObject))
  Http("https://mavz71dubi.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/").postData(stringifiedData).asString
}

def updateAttribute(attribute : Attribute_Metadata) = {
  val jsonObject = convertAttributeToJson(attribute)
  val stringifiedData = compact(render(jsonObject))
  Http(s"https://mavz71dubi.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/${attribute.name}").postData(stringifiedData).method("put").asString
}

for (attribute <- attributeList) {
  val response = updateAttribute(attribute)
  println(response.code + " " + response)
}

val test_attribute_12 = new Attribute_Metadata("test_attribute_13",
  "boolean","HDE CAS","Srinivas Pachari","2019-05-03T11:12:00.000Z",
  "Srinivas Pachari", true,"2019-05-21T10:15:16.000Z","2019-05-21T10:15:16.000Z")

val jsonObject1 = convertAttributeToJson(test_attribute_12)
val stringifiedData1 = compact(render(jsonObject1))
Http(s"https://mavz71dubi.execute-api.us-west-2.amazonaws.com/lab/api/v1/attributes/test_attribute_12").postData(stringifiedData1).method("put").asString


// ALL WORKING NOW

//Test isActive checks
