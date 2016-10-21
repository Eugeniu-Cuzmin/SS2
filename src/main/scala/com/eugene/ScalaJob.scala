package com.eugene

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class ScalaJob(sc: SparkContext) {
  def run(filePath: String) : RDD[(String, String, String)] = {
    //pass the file
    val file = sc.textFile(filePath);
    //find values in every raw
    val values = file.map{
      dataRaw =>
      val p = dataRaw.split("[|]",-1)
      (p(1), ScalaJob.processDate(p(2)), p(32))
    }

    //count occurrences of same number
//    val result = processData(values)

    return values
  }

}

object ScalaJob{
  //determine part of day
  def processDate(s: String) : String = {
    val date = DateTime.parse(s, DateTimeFormat.forPattern("yyyyMMddHHmmss"));
    val hour = date.getHourOfDay
    val h = hour match{

      //for morning
      case hour if 6 to 11 contains(hour) => "1"
      //for day
      case hour if 12 to 18 contains(hour) => "2"
      //for evening
      case hour if 19 to 23 contains(hour) => "3"
      //for night
      case _ => "4"
    }
    return h
  }
}