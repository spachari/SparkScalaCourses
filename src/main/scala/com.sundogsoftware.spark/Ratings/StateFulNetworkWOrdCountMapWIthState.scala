package com.sundogsoftware.spark.Ratings







import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
  * Counts words cumulatively in UTF8 encoded, '\n' delimited text received from the network every
  * second starting with initial value of word count.
  * Usage: StatefulNetworkWordCount <hostname> <port>
  *   <hostname> and <port> describe the TCP server that Spark Streaming would connect to receive
  *   data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example
  *    `$ bin/run-example
  *      org.apache.spark.examples.streaming.StatefulNetworkWordCount localhost 9999`
  */

object StateFulNetworkWOrdCountMapWIthState {
  def main(args: Array[String]) {


    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[8]").setAppName("StatefulNetworkWordCount")


    val ssc = new StreamingContext(conf, Seconds(10))


    //val sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount")
    // Create the context with a 1 second batch size
    //val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")

    // Initial state RDD for mapWithState operation
    val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))

    // Create a ReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc')

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))

    // Update the cumulative count using mapWithState
    // This will give a DStream made of state (which is the cumulative count of the words)
    val mappingFunc = (key: String, value_one: Option[Int], state: State[Int]) => {

      println("value in value_one ... " + value_one.getOrElse(0))
      println("value in state ... " + state.getOption().getOrElse())
      val sum = value_one.getOrElse(0) + state.getOption.getOrElse(0)
      val maxi = value_one.getOrElse(0) max state.getOption.getOrElse(0)
      val mini = value_one.getOrElse(0) min state.getOption.getOrElse(0)
      val first = if (state.exists())
        {
          state.getOption().getOrElse(0)
        }
      else
        {
          value_one.getOrElse(0)
        }

      val output = (key, sum, maxi, mini, first)
      state.update(sum)
      //state.update(maxi)
      //state.update(mini)
      state.update(first)
      output
    }

    val stateDstream = wordDstream.mapWithState(
      StateSpec.function(mappingFunc))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()

  }
}