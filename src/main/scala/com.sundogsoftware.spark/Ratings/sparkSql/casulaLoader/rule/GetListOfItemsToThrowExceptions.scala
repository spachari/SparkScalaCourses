package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.rule

import scala.util.Try

class NegativeNumberException(message : String) extends Exception(message)

object GetListOfItemsToThrowExceptions extends App {

  val list = List(1, 2, 3, 0, 5, 0, 3)

  def divide100ByListItem(i : Int) = {
    100 / i
  }

  val output = for (elem <- list) yield {
    if (Try(divide100ByListItem(elem)).isFailure) Some(elem) else None
  }

  val outputList = output.flatten

  if (outputList.isEmpty)
    println("All attributes are loaded properly")
  else {
    val message = s"The following items have not been loaded ${outputList}"
    throw new NegativeNumberException(message)
  }

  /*
  def getValuefromTry(item : (Try[Any],Int)) = {
    item match {
      case x : (Failure[Any],Int) => Some(x._2)
      case x : (Success[Int],Int) => None
    }
  }

  output.map(x => getValuefromTry(x)).foreach(println)

  output.flatMap(x => getValuefromTry(x)).foreach(println)

  val result = output.flatMap(x => getValuefromTry(x))

  result.foreach(println)

*/
}
