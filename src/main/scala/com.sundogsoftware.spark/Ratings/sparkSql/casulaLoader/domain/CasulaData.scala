package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain

case class CasulaData(
                  trvl_acc_email_addr : String,
                  attributes : Array[Attribute],
                  load_timestamp : String
                )
