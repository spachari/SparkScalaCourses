package com.sundogsoftware.spark.Ratings

import org.apache.log4j._
import org.apache.spark.SparkContext

object AmountSpentByEachCustomer extends App {


  def parseLines (string: String) = {
    val line = string.split(",")
    val customerId = line(0)
    val amount = line(2).toFloat
    (customerId, amount)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "AmpuntSpentByEachCustomer")

  val lines = sc.textFile("/Users/spachari/Desktop/Spark-learning/SparkScala/SparkScalaCode/customer-orders.csv")

  val customerAndSpendingAnounts = lines.map(parseLines)

  val totalAmountSpentbyCustomer = customerAndSpendingAnounts.reduceByKey((x,y) => x + y)



  val pi = Math.PI
  println(f"$pi%.2f")

  for (c <- totalAmountSpentbyCustomer) {
    val customer = c._1
    val totalAmount = f"${c._2}%.2f"
    println(s"${customer} spent ${totalAmount}")
  }

  //Using sortByKey
  println("Top 5 customers who spent the most")
  for ( c <- totalAmountSpentbyCustomer.map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2,x._1)).take(5))
    {
      val customer = c._1
      val totalAmount = f"${c._2}%.2f"
      println(s"${customer} spent ${totalAmount}")
    }

  //Using sortBy
  println("Top 5 customers who spent the least")
  for (c <- totalAmountSpentbyCustomer.sortBy(_._2,true).take(5))
    {
      val customer = c._1
      val totalAmounts = f"${c._2}%.2f"
      println(s"${customer} spent ${totalAmounts}")
    }

}
