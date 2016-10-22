package com.eugene

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class ScalaJob(sc: SparkContext) {
  import ScalaJob._

  def run(cdrPath: String): RDD[(String, String)] = {
    val valuesCdr = sc.textFile(cdrPath)
      .map(_.split("\\|"))
      .map(p => (p(1), processType(processTime(p(2)), p(32))))

    val result = valuesCdr.groupByKey().mapValues(countValues)

    return result
  }
}

object ScalaJob {
  val dayParts = Map((6 to 11) -> 1, (12 to 18) -> 2, (19 to 23) -> 3, (0 to 5) -> 4)

  def processTime(s: String): Int = {
    val hour = DateTime.parse(s, DateTimeFormat.forPattern("yyyyMMddHHmmss")).getHourOfDay
    dayParts.filterKeys(_.contains(hour)).values.head
  }

  def processType(dayPart: Int, s: String): Int = s match {
    case "S" => 2 * dayPart - 1
    case "V" => 2 * dayPart
  }

  def countValues(l: Iterable[Int]): String = {
    (1 to 8).map(i => (i + "_" + l.count(_ == i))).mkString(",")
  }
}