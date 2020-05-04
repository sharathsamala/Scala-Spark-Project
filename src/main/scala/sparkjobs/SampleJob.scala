package sparkjobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import utils.RunMode.RunMode
import utils.SparkFactory._
import utils.TestUtils.runModeFromOS

object SampleJob extends Logging {

  val runMode : RunMode = runModeFromOS()

  def main(args: Array[String]): Unit = {

    log.info("Creating spark session")
    val spark: SparkSession = createSparkSession(runMode, "SampleJob")

    log.debug("Reading csv from datasets in test")

    val csvDf = spark.read.csv("src/test/resources/testdata/sample_emp_data.csv")

    csvDf.show(10, false)
  }

}
