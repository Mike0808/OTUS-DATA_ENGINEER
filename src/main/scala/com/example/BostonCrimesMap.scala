package com.example

import org.json4s.DefaultFormats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object BostonCrimesMap extends App {


  val spark = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._

  def sc = spark.sparkContext

  implicit val df = DefaultFormats

  val csvCrimeFacts= args(0)
  val csvOffenseCodes= args(1)
  val parquetOutputFolder= args(2)

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csvCrimeFacts)

  println("#########################################")
  println("# Выберите район из списка:             #")
  println("#########################################")

  crimeFacts.select("DISTRICT").show()
  val inputTypeDistrict = scala.io.StdIn.readLine("Choose and type your District: ")
 println("You choose: " + inputTypeDistrict.toUpperCase)
    crimeFacts.createOrReplaceTempView("crimeFacts")
    val crimes_total = spark.sql("select COUNT(DISTRICT) as total_crimes_count ,DISTRICT from crimeFacts Where DISTRICT = '" + inputTypeDistrict.toUpperCase + "' group by DISTRICT")
      .repartition(1)
      .write
      .format("parquet")
      .mode("append")
      .save(parquetOutputFolder)

    val crimes_total_val = crimeFacts
      .select("DISTRICT")
      .filter("DISTRICT='" + inputTypeDistrict.toUpperCase + "'")
      .count()
      .toString

  println("#########################################")
  println("# В районе " + inputTypeDistrict.toUpperCase + " - " +   crimes_total_val  + " преступлений    #")
  println("#########################################")



    val percentile_approx = spark.sql("select DISTRICT, percentile_approx(crimes_count, 0.5) " +
      "as medianDistrict, YEAR, MONTH " +
      "FROM " +
      "(select COUNT(DISTRICT) as crimes_count ,DISTRICT, MONTH, YEAR " +
      "FROM crimeFacts group by DISTRICT, YEAR,MONTH ORDER BY YEAR, MONTH) " +
      "WHERE DISTRICT='" + inputTypeDistrict.toUpperCase + "' group by DISTRICT, YEAR, MONTH")

  percentile_approx.show()
  percentile_approx
    .repartition(1)
  .write
  .format("parquet")
    .mode("append")
    .save(parquetOutputFolder)

  val offenseCodes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvOffenseCodes)

    offenseCodes.createOrReplaceTempView("offenseCodes")
    val splitedViewsDF = offenseCodes.withColumn("crime_type", split($"NAME", "-").getItem(0)).drop($"NAME")

  import org.apache.spark.sql.functions.broadcast

  val offenseCodesBroadcast = broadcast(splitedViewsDF)

  val crimeFactsWithBroadcast = crimeFacts
    .join(offenseCodesBroadcast, offenseCodesBroadcast("CODE") === crimeFacts("OFFENSE_CODE"))

  val frequent_crime_types = crimeFactsWithBroadcast
    .select("DISTRICT","crime_type")
    .filter("DISTRICT='" + inputTypeDistrict.toUpperCase + "'")
    .groupBy("crime_type")
    .count()
    .orderBy($"count".desc)
    .select($"crime_type")

 val ls = frequent_crime_types.take(3).mkString(", ").foreach(e=>print(e))
  frequent_crime_types
    .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    .save(parquetOutputFolder)

  val lat = spark.sql("SELECT AVG(Lat) as lat " +
    "FROM " +
    "crimeFacts WHERE DISTRICT='" + inputTypeDistrict.toUpperCase + "' group by DISTRICT")

  lat.show()
   lat
     .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    .save(parquetOutputFolder)

  val lng = spark.sql("SELECT AVG(Long) as lng " +
    "FROM " +
    "crimeFacts WHERE DISTRICT='" + inputTypeDistrict.toUpperCase + "' group by DISTRICT")
lng.show()
  lng
    .repartition(1)
    .write
    .format("parquet")
    .mode("append")
    .save(parquetOutputFolder)



}
