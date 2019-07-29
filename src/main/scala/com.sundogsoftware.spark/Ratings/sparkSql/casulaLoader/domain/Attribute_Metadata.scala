package com.sundogsoftware.spark.Ratings.sparkSql.casulaLoader.domain

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
