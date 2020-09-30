import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object unPivot {

  def melt(
            df:DataFrame, requiredColumn: List[String], reArrangeColumn: List[String],
            var_name: String = "Date", value_name: String = "Value") : (DataFrame, Int) = {

    /**
     * val df2 = df1.select($"geo_region", expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico, 'USA', USA) as (Date, Value)")).where("Value is not null")
     * we can do in this way but here the number of column is not fixed so we need to update column number daily
     */

    try {
      /**
       * Create array<struct<variable: str, value: ...>>
       * named_struct(Date, 2020-01-13 AS `Date`, NamePlaceholder(), 2020-01-13 AS `Value`)
       */
      val _vars_and_vals = array((for (c <- reArrangeColumn) yield {
        struct(lit(c).alias(var_name), col(c).alias(value_name))
      }): _*)

      /**
       * Add to the DataFrame and explode
       * [2020-01-13, 100.0]
       */
      val _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))


      /**
       * _vars_and_vals[Date] AS `Date`
       * _vars_and_vals[Value] AS `Value`
       */
      val cols = requiredColumn.map(col) ++ {
          for (x <- List(var_name, value_name)) yield {
            col("_vars_and_vals")(x).alias(x)
          }
        }

      /**
       * select required column and date, value column
       */
      (_tmp.select(cols: _*), 1)

    }

    catch {
      case e:Exception => (df, 0)
    }
  }

  def similar(list1:List[String]): Int ={

    if(list1.length == 6){
    val requiredList = List("geo_type", "region", "transportation_type", "alternative_name", "sub-region", "country")
    val count = requiredList.foldLeft(0)((x,y)=>{
      if(list1.contains(y))
        0
      else
        x+1
    })
    count
    }
    else
      1
  }


  def isDate(str:String):Int ={

    val date =  try {
      DateTime.parse(str, DateTimeFormat.forPattern("YYYY-MM-DD"))
      1
    }
    catch {
      case e:Exception => 0
    }
    date
  }
}

