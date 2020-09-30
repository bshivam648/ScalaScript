import org.junit.jupiter.api.Test
import org.apache.spark.sql.SparkSession



class testCase {


  val currentDate = java.time.LocalDate.now
  val DownloadFile = "Downloaded-" + currentDate + ".csv"
  /**
   * test for testing list of required columns
   */
  @Test
  def test1():Unit = {
    val list1 = List("geo_type", "region", "transportation_type", "alternative_name", "sub-region", "country")
    assert(unPivot.similar(list1) == 0)

    val list2 = List( "region","geo_type", "transportation_type", "alternative_name", "sub-region", "country")
    assert(unPivot.similar(list2) == 0)

    val list3 = List( "region","geo_type", "transportation_type", "alternative_name")
    assert(unPivot.similar(list3) != 0)

    val list4 = List( "region","geo_type", "transportation_type", "alternative_name", "sub-region", "country", "Date")
    assert(unPivot.similar(list4) != 0)

  }

  /**
   * test for testing date format
   */
  @Test
  def test2():Unit ={
    val date1 = "2020-12-20"
    assert(unPivot.isDate(date1) == 1)

    val date2 = "2020-20-12"
    assert(unPivot.isDate(date2) != 1)

    val date3 = "dateColumn"
    assert(unPivot.isDate(date3) != 1)
  }

  /**
   * test for testing content of CSV file
   */
  @Test
  def test3():Unit ={
    assert(urlInfo.getURL() != "None")
  }

  @Test
  def test4():Unit ={
    val spark = SparkSession.builder().appName("scalaScript").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val dataFrame = spark.read.option("header",true).csv(DownloadFile)

    val allColumns = dataFrame.columns.toList
    val requiredColumn = allColumns.slice(0, 6)
    val reArrangeColumn = allColumns.slice(6, allColumns.length)
    assert(unPivot.melt(dataFrame, requiredColumn, reArrangeColumn)._2 == 1)

  }
}
