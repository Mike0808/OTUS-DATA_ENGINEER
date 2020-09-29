package com.example

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse


object JsonReader extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def sc = spark.sparkContext

  implicit val df = DefaultFormats
  val jsonfile = args(0)
  case class WineMagData(
                          id: Option[BigInt],
                          country: Option[String],
                          points: Option[BigInt],
                          title: Option[String],
                          variety: Option[String],
                          winery: Option[String]
                        )

  val js = sc.textFile(jsonfile).cache()
  js.foreach { e =>
    val s = parse(e).extract[WineMagData]
    println(s)
  }

}
