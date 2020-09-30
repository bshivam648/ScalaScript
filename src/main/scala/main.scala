import java.io.{BufferedWriter, FileWriter}
import org.apache.spark.sql.SparkSession
import scala.util.parsing.json._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

object main {

  val currentDate = java.time.LocalDate.now
  val DownloadFile = "Downloaded-" + currentDate + ".csv"
  val SavedFile = "Saved-" + currentDate

  def main(args: Array[String]): Unit = {

    val content = urlInfo.getURL()
    var check = 0                     // flag

    if (content != "None") {
      val newFile = new BufferedWriter(new FileWriter(DownloadFile))
      newFile.write(content)


      /**
       * create a spark session
       */
      val spark = SparkSession.builder().appName("scalaScript").master("local[*]").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      val readCSV = spark.read.option("header", true).csv(DownloadFile).toDF()

      val allColumns = readCSV.columns.toList
      val requiredColumn = allColumns.slice(0, 6)
      val reArrangeColumn = allColumns.slice(6, allColumns.length)

      /**
       * check the column name is same as we require
       * check the format of next column which is date
       */
      val varifyColumn = unPivot.similar(requiredColumn)
      val varifyDate = unPivot.isDate(allColumns(6))

      if (varifyColumn == 0 && varifyDate == 1) {
        val finalDF = unPivot.melt(readCSV, requiredColumn, reArrangeColumn)

        if(finalDF._2 == 1) {
          finalDF._1.write.format("com.databricks.spark.csv").parquet(SavedFile)
          println("Successfully Saved")
          check = 1
        }
      }
    }
    if (check == 0)
      print("Can't saved, got some error")
  }
}
