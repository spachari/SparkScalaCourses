package com.sundogsoftware.spark.Ratings.sparkSql.customdatatype

import scala.io.Source

object ReadFromResourcesFile extends App {
    val paths = getClass.getResource("/application_lab.conf")

    val data = Source.fromFile(paths.getPath).getLines().toList

  val output = data.filter(_.startsWith("endPointURL")).map{case x => x.split("::")(1)}.mkString

  println(output)


    //val folder = Source.fromInputStream(paths)

}
