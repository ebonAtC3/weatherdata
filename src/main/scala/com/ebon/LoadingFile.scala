package com.ebon


import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.ebon.DataValidation._
import com.ebon.Utils._


object LoadingFile {
  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map {
      line =>
        val tokens = line.split('\t')
        val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0, ZoneId.systemDefault())
        val station = tokens(3)
        val pressure = if (validateDouble(tokens(8)).isEmpty) Double.NaN else tokens(8).toDouble
        val humidity = if (validateDouble(tokens(5)).isEmpty) Double.NaN else tokens(5).toDouble
        val tMax = if (validateDouble(tokens(5)).isEmpty) Double.NaN else tokens(6).toDouble
        val tMin = if (validateDouble(tokens(5)).isEmpty) Double.NaN else tokens(7).toDouble
        val rain = if (validateDouble(tokens(5)).isEmpty) Double.NaN else tokens(4).toDouble
        val tempList = List(tMin,tMax)
        val temp = average(tempList)
        Row(Timestamp.from(dt.toInstant), station, pressure, humidity, temp, rain)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, nullable = true),
      StructField("station", StringType, nullable = true),
      StructField("pressure", DoubleType, nullable = true),
      StructField("humidity", DoubleType, nullable = true),
      StructField("temp", DoubleType, nullable = true),
      StructField("rain", DoubleType, nullable = true)
    )

    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def loadWeatherStations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map {
      line =>
        val tokens = line.split('\t')
        val iata = tokens(0)
        val station = tokens(1)
        val longitude = if (validateDouble(tokens(2)).isEmpty) Double.NaN else tokens(2).toDouble
        val latitude = if (validateDouble(tokens(3)).isEmpty) Double.NaN else tokens(3).toDouble
        val country = tokens(4)
        Row(iata, station, longitude, latitude, country)
    }
    val fields = Seq(
      StructField("iata", StringType, nullable = true),
      StructField("station", StringType, nullable = true),
      StructField("longitude", DoubleType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("country", DoubleType, nullable = true)
    )

    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }
}
