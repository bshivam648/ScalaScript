import scala.util.parsing.json.JSON

object urlInfo {

  def getURL():String ={

    try
      {
        val url = "https://covid19-static.cdn-apple.com/covid19-mobility-data/current/v3/index.json"
        val cont = scala.io.Source.fromURL(url)
        val JsonString = cont.mkString
        cont.close()
        val json_data = JSON.parseFull(JsonString)
        val map = json_data match {
          case Some(map: Map[String, Map[String,String]]) => map
          case None => Map[String, Map[String,String]]()
          case _ => Map[String, Map[String,String]]()
        }

        val basePath = map.get("basePath").get.asInstanceOf[String]
        val regions = map.get("regions").get.asInstanceOf[Map[String, Map[String, String]]]
        val en_us = regions.get("en-us").get
        val csvPath  = en_us.get("csvPath").get

        val tempURL = basePath + csvPath
        val finalURL = "https://covid19-static.cdn-apple.com" + tempURL
        val content = getContent(finalURL)
        content
      }
    catch {
      case e:Exception => "None"
    }
  }

  def getContent(url:String):String ={

    try {
      val cont = scala.io.Source.fromURL(url)
      val content = cont.mkString
      cont.close()
      content
    }
    catch {
      case e:Exception => "None"
    }
  }
}
