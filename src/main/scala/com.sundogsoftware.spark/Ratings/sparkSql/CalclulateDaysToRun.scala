package com.sundogsoftware.spark.Ratings.sparkSql

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.joda.time.LocalDate

object CalclulateDaysToRun extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

  /*
  def jsonToDataFrame(jsonDF : DataFrame, schema : StructType = null): DataFrame = {
    val reader = spark.read //this gives a dataframereader
    Option(schema).foreach(reader.schema)
    reader.json(jsonDF)
  }
  */

  case class Data(dateToRun : Date, daysToRun : Int) {
    override def toString: String = s"$dateToRun - $daysToRun"
  }

  def makeArray(data : Data) = {
    val dateToRunDate = LocalDate.parse(data.dateToRun.toString)

    val listofDates : List[LocalDate] = List.empty

    val output = for (i <- 1 to data.daysToRun) yield  dateToRunDate.plusDays(i) :: listofDates

    output.flatten
  }


  def doSomething(s : Array[String])(implicit spark : SparkSession) = {
    import spark.implicits._
    spark.sparkContext.parallelize(s).toDF()
  }

  //implicit val spark = SparkSession

  doSomething(Array("Srinivas","Pachari"))

  def makeDataDFWay(dataFrame : DataFrame) = {
    val dateToRun = dataFrame.select("dateToRun")
  }

  implicit val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("partitionTutorial")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  val file = spark.read.text("/Users/spachari/Desktop/missingbookings.txt").select("*").toDF("data")

  val cleansedDF = file.select(substring(col("data"),1,10).as("dateToRun"), trim(substring(col("data"),11, 2), "\t").as("daysToRun"))
    .where(col("dateToRun").startsWith("20")).toDF("dateToRun", "daysToRun")

  cleansedDF.printSchema()

  val missingDaysDF = cleansedDF.select(col("dateToRun").cast(DateType), col("daysToRun").cast(IntegerType)).where(col("daysToRun") > 0).rdd.map(x => Data(x.getDate(0), x.getInt(1))).collect()

  val daysToRun = missingDaysDF.map(x => makeArray(x)).flatten.foldLeft( List[LocalDate]()){case(xs,ys) => xs :+ ys}

  def convertDateToStep(x : String) = {
    s"""aws emr add-steps --cluster-id j-7XQPVECXHZ1C  --steps Type=Spark,Name=\"booking job ${x}\",ActionOnFailure=CONTINUE,Args=[--deploy-mode,client,--driver-memory,10g,--class,com.hotels.hde.cas.bookingsummary.aggregation.BookingSummaryAggregationDriver,s3://hcom-data-prod-bix-meta/hde-cas-booking-summary-aggregation/hde-cas-booking-summary-aggregation-spark.jar,--configFile,s3://hcom-data-prod-bix-meta/hde-cas-booking-summary-aggregation/application_prod.conf,--bookingSummaryAttributesTable,roads.hde_booking_summary_attributes,--customerEmailMapTable,bix_cross_device.travel_account_email_map,--bookingAttributesTable,bix_cas.booking_attributes,--startDate,${x},--endDate,${x},--acquisitionInstant,20190226T070211Z,--tableFormat,AVRO]"""
  }

  val script = daysToRun.map(x => convertDateToStep(x.toString()))

  for (elem <- script) {println(elem)}

}
