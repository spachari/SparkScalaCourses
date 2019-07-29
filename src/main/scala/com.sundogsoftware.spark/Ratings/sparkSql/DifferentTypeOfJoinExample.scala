package com.sundogsoftware.spark.Ratings.sparkSql

import org.apache.spark.sql._

object SparkSandbox extends App {

  case class Row(id: Int, value: String)

  private[this] implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val r1 = Seq(Row(1, "A1"), Row(2, "A2"), Row(3, "A3"), Row(4, "A4")).toDS()
  val r2 = Seq(Row(3, "A3"), Row(4, "A4"), Row(4, "A4_1"), Row(5, "A5"), Row(6, "A6")).toDS()

  val joinTypes = Seq("inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi", "left_anti")

  joinTypes foreach {joinType =>
    println(s"${joinType.toUpperCase()} JOIN")
    r1.join(right = r2, usingColumns = Seq("id"), joinType = joinType).orderBy("id").show()
  }
}