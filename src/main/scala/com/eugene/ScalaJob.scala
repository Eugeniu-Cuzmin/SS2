package com.eugene

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class ScalaJob(sc: SparkContext) {
  import ScalaJob._

  def run(cdrPath: String, dimPath: String, cachePath: String): RDD[(String, (String, String))] = {
    val valuesCache = sc.textFile(cachePath).map(_.split(" ")).map(p => (p(0), ""))

    val valuesCdr = sc.textFile(cdrPath)
      .map(_.split("\\|"))
      .map(p => (p(1), processType(processTime(p(2)), p(32))))
      .groupByKey()
      .mapValues(countValues)

    val newValueCdr = valuesCdr.subtractByKey(valuesCdr.subtractByKey(valuesCache))

    val result = sc.textFile(dimPath)
      .map(_.split("\u0001"))
      .filter(hasAS)
      .map(p => (p(1),(p(0) + "," + p(3) + "," + p(4))))
      .join(newValueCdr)

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

  def hasAS (v : Array[String]) : Boolean = {
    v(7).equals("S") || v(7).equals("A")
  }
}