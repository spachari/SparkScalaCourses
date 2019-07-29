package com.sundogsoftware.spark.Ratings.utilities


import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext

@SerialVersionUID(1000L)
object StreamingErrorHandler extends Serializable with LazyLogging {

  // a thread spun off to call ssc.stop when it's time
  def monitorApplication(sc: SparkContext): Unit = {
    new Thread("monitor-streaming-application") {
      override def run(): Unit = {

        while(!sc.isStopped) {
          Thread.sleep(1 * 1000)
        }
        logger.info("Aborting application as sparkContext is stopped.")
        abort()
      }
    }.start()
  }

  def stopStreamingContext(ssc : StreamingContext) = {
    new Thread("stop-streaming-context") {
      override def run(): Unit = {
        logger.info("******** Streaming application have not reecived data for 60 mins")
        abort()
      }
    }.start()
  }

  // abort helper
  private def abort(): Unit = {
    logger.info("stopping Streaming Context right now")
    StreamingContext
      .getActive()
      .fold(
        logger.error("No active Streaming Context found")
      )(_.stop())
    logger.info("Streaming Context is stopped!!!!!!!")
    sys.exit(0)
  }
}
