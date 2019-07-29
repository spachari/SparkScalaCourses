import org.apache.log4j.{Level, Logger}



Logger.getLogger("org").setLevel(Level.ERROR)
val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

object sparkWorksheet extends App {



}
