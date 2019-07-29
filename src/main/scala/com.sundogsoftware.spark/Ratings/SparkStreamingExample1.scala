package com.sundogsoftware.spark.Ratings

import org.apache.spark.SparkContext

object SparkStreamingExample1 extends App {

  import org.apache.spark.streaming.twitter.TwitterUtils
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  def configureTwitterCredentials(consumerKey: String,
                                  consumerSecret: String,
                                  accessToken: String,
                                  accessTokenSecret: String) {
    val configs = Seq("consumerKey" -> consumerKey,
      "consumerSecret" -> consumerSecret,
      "accessToken" -> accessToken,
      "accessTokenSecret" -> accessTokenSecret).toMap
    val trimmedConfigs = configs.mapValues(_.trim)
    configs.foreach{ case(key, value) =>
      require(value.nonEmpty, s"""Error setting authentication - value for  $key  not set""")
      val fullKey = "twitter4j.oauth." + key
      System.setProperty(fullKey, value)    }
  }

  configureTwitterCredentials("aPrGDjfv78kbpBPOp3RZ3BfMn","beqczj0KiXhPkjIdEzaGBpO02t2hRRwXWhuYfzKaxkul5kwXHJ",
    "887022661888561152-uid3YP1nOButwerJpqJu8gAuDMsiFgS","HOWe2jWTDZ37gfLct4LVAUZwCJHYhL688lo8QsgPmPp6G")

  val sc = new SparkContext()
  val ssc = new StreamingContext(sc, Seconds(5))

  val filters = Array("music")
  val twitterStream = TwitterUtils.createStream(ssc, None, filters)
  val hashTags = twitterStream.flatMap(status => status.getText.split(
    " ").filter(_.startsWith("#")))

  val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    .map{case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))

  topCounts60.print()

  val tumblingSums = hashTags.window(Seconds(60)).map(hashTag => (hashTag,1))
                              .reduceByKey(_ + _)

}
