package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.exceptions

abstract class CasulaLoaderExceptions extends Exception {
  val message : String
  def printMessage(errorCode : Int) = {
    println("ERROR : " + message)
    println(s"Exiting with error message $errorCode")
    sys.exit(errorCode)
  }
}
