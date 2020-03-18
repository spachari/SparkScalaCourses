name := "SparkScalaCourses"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += "MavenRepository" at "https://mvnrepository.com/"

libraryDependencies ++= Seq(
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.9",
  "joda-time" % "joda-time" % "2.9.9",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-graphx_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalatest" %% "scalatest" % "2.2.1"
  ,"org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
  "org.apache.bahir" % "spark-streaming-twitter_2.11" % "2.2.0",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "org.json4s" %% "json4s-native" % "3.6.0-M2",
  "org.json4s" %% "json4s-jackson" % "3.6.0-M2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.0"
  
).map(_.exclude("net.jpountz.lz4","lz4"))


dependencyOverrides += ("com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9")


/*
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  //case PathList(ps @ _*) if ps.last endsWith ".html" => println(ps); MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".thrift" => println(ps); MergeStrategy.last
  case PathList(ps @ _*) if ps.last endsWith ".xml" => println(ps); MergeStrategy.last
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
  case x if x.endsWith(".html") => println(x); MergeStrategy.discard // More bumf
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  case x if x.contains("avro")  => MergeStrategy.last
  case x if x.contains("datnucleus")  => MergeStrategy.last
  case x if x.endsWith(".class") => MergeStrategy.first

  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
  case x =>
    val oldStrategy = (mergeStrategy in assembly).value
    oldStrategy(x)
}

*/