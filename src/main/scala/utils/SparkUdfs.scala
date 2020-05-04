package utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SparkUdfs extends Logging {

  // Sample udf to remove space in a string column, For demonstration only
  def removeSpaceUdf(input: String): String = {
    input.replaceAll(" ","")
  }

  def registerUDFs()(implicit spark: SparkSession) : Unit = {
    try {
      spark.sqlContext.udf.register("removeSpace", removeSpaceUdf _)
      log.info("UDFs registered successfully")
    }
    catch {
      case e: Exception => log.error("Unable to register UDFs")
    }
  }

}
