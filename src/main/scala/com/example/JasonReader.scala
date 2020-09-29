package com.example

import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.util.Try

object JsonReader extends App {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  def sc = spark.sparkContext

  implicit val df = DefaultFormats
  val jsonfile = args(0)
  case class WineMagData(
                          id: BigInt,
                          country: String,
                          points: BigInt,
                          title: String,
                          variety: String,
                          winery: String
                        )

  val js = sc.textFile(jsonfile).cache()
  js.collect().foreach { e =>
    val s = Try(parse(e).extract[WineMagData]).toOption
    println(s)
  }
}
