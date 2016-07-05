package com.ebon


import com.ebon.Spark._
import com.ebon.IO._
import com.ebon.Utils._
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import com.cloudera.sparkts._
import com.cloudera.sparkts.models.ARIMA
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import sqlContext.implicits._


object TimeSeries {
  def runTimeSeries(): Unit ={

    // TODO: Pass as a user parameter
    val extraDays = 731

    val from = DateTime.parse("2009-04-30 00:00:00", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val to = DateTime.parse("2011-04-30 00:00:00", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val dateRangeRdd = sc.parallelize(calculateDateRange(from, to, extraDays))

    println("######################### Loading Baseline Historical Data")
    val weatherData = loadObservations(sqlContext, "../weatherdata/data/input/NZ_Weather_History.tsv")

    println("######################### Loading Weather Stations Information")
    val weatherStations = loadWeatherStations(sqlContext, "../weatherdata/data/input/NZ_Weather_Station.tsv")

    // Create an daily DateTimeIndex over 30th of April 2009 and 30th of April September 2011
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2009-04-30T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2011-04-30T00:00:00"), zone),
      new DayFrequency(1))

    println("######################## Aligning weather data on DateTimeIndex creating TimeSeriesRDD")
    val pressureTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, weatherData, "timestamp", "station", "pressure")
    val humidityTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, weatherData, "timestamp", "station", "humidity")
    val tempTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, weatherData, "timestamp", "station", "temp")
    val rainTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, weatherData, "timestamp", "station", "rain")

    // Cache rdds in memory
    pressureTsrdd.cache()
    humidityTsrdd.cache()
    tempTsrdd.cache()
    rainTsrdd.cache()
    dateRangeRdd.cache()

    println("######################### Impute missing values utilising linear interpolations")
    val pressureFilled = pressureTsrdd.fill("linear")
    val humidityfilled = humidityTsrdd.fill("linear")
    val tempfilled = tempTsrdd.fill("linear")
    val rainfilled = rainTsrdd.fill("linear")

    println("########################## Applying Arima Model and generating predictions")
    val pressureForecastTsrdd = pressureFilled.map(x => (x._1, x._2, ARIMA.fitModel(1,0,1, x._2).forecast(x._2, extraDays)))
    val humidityForecastTsrdd = humidityfilled.map(x => (x._1, x._2, ARIMA.fitModel(1,0,1, x._2).forecast(x._2, extraDays)))
    val tempForecastTsrdd = tempfilled.map(x => (x._1, x._2, ARIMA.fitModel(1,0,1, x._2).forecast(x._2, extraDays)))
    val rainForecastTsrdd = rainfilled.map(x => (x._1, x._2, ARIMA.fitModel(1,0,1, x._2).forecast(x._2, extraDays)))

    println("######################### Creating dataframes and registering tables")
    tempForecastTsrdd.map(x =>  x._3.toArray).toDS().toDF().registerTempTable("temp_table")
    pressureForecastTsrdd.map(x => x._3.toArray).toDS().toDF().registerTempTable("pressure_table")
    humidityForecastTsrdd.map(x =>  x._3.toArray).toDS().toDF().registerTempTable("humidity_table")
    rainForecastTsrdd.map(x =>  x._3.toArray).toDS().toDF().registerTempTable("rain_table")
    weatherStations.select("iata", "longitude", "latitude").registerTempTable("station_info_table")
    val dateRangeDf = dateRangeRdd.map(x => x.toString).toDF("date")
    val iataDf = tempForecastTsrdd.map(x => x._1).toDF()
    val numStations = iataDf.count().toInt
    // TODO: make it dynamic
    val stationDf = weatherData.select("station").unionAll(weatherData.select("station")).cache()

    println("######################### Number of weather stations: " + numStations)

    // TODO: make it dynamic
    dateRangeDf.unionAll(dateRangeDf).unionAll(dateRangeDf).unionAll(dateRangeDf).unionAll(dateRangeDf).registerTempTable("date_table")

    val stationSchema = StructType(
      Array(StructField("station_id", LongType, nullable=false),StructField("station",StringType,nullable=false))
    )
    val stationRdd = stationDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(stationRdd, stationSchema).registerTempTable("station_table")

    val tempDf =  sqlContext
                    .sql("select explode (value) as temp from temp_table")
    val tempSchema = StructType(
      Array(StructField("temp_id", LongType, nullable=false),StructField("temp",DoubleType,nullable=false))
    )

    val tempRdd = tempDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(tempRdd, tempSchema).registerTempTable("temp_table2")

    val pressureDf =  sqlContext
      .sql("select explode (value) as pressure from pressure_table")
    val pressureSchema = StructType(
      Array(StructField("pressure_id", LongType, nullable=false),StructField("pressure",DoubleType,nullable=false))
    )
    val pressureRdd = pressureDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(pressureRdd, pressureSchema).registerTempTable("pressure_table2")

    val humidityDf =  sqlContext
      .sql("select explode (value) as humidity from humidity_table")
    val humiditySchema = StructType(
      Array(StructField("humidity_id", LongType, nullable=false),StructField("humidity",DoubleType,nullable=false))
    )
    val humidityRdd = humidityDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(humidityRdd, humiditySchema).registerTempTable("humidity_table2")

    val rainDf =  sqlContext
      .sql("select explode (value) as humidity from rain_table")

    val rainSchema = StructType(
      Array(StructField("rain_id", LongType, nullable=false),StructField("rain",DoubleType,nullable=false))
    )
    val rainRdd = rainDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(rainRdd, rainSchema).registerTempTable("rain_table2")

    val dateDf =  sqlContext
      .sql("select date as local_date from date_table")
    val dateSchema = StructType(
      Array(StructField("date_id", LongType, nullable=false), StructField("local_date",StringType,nullable=true))
    )
    val dateRdd = dateDf.rdd.zipWithUniqueId.map{
      case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq)}
    sqlContext.createDataFrame(dateRdd, dateSchema).registerTempTable("date_table2")

    // Registering Utils.roundDouble and Utils.doubleToInt functions for sql use
    sqlContext.udf.register("roundDouble", roundDouble(_:Double))
    sqlContext.udf.register("doubleToInt", doubleToInt(_:Double))

    println("######################### Joining dataframes and generating conditions")

    val combinedDf = sqlContext.sql(
      "SELECT station " +
        "     ,latitude " +
        "     ,longitude " +
        "     ,local_date" +
        "     ,CASE WHEN rain > 20 AND temp > 0 THEN " +
        "       'Rain' " +
        "     WHEN rain > 20 AND temp <= 0 THEN " +
        "       'Snow' " +
        "     WHEN rain <= 20 AND pressure > 1000 THEN " +
        "       'Mostly Sunny' " +
        "     WHEN rain = 0 AND pressure <= 1000 THEN " +
        "       'Overcast' " +
        "     ELSE " +
        "       'Sunny' " +
        "     END AS condition" +
        "     ,roundDouble(temp) AS temperature " +
        "     ,roundDouble(pressure) AS pressure " +
        "     ,doubleToInt(humidity) AS humidity " +
        "FROM date_table2 dt " +
        "LEFT JOIN rain_table2 rt ON rt.rain_id = dt.date_id " +
        "LEFT JOIN temp_table2 tt ON tt.temp_id = dt.date_id " +
        "LEFT JOIN pressure_table2 pt ON pt.pressure_id = dt.date_id " +
        "LEFT JOIN humidity_table2 ht ON ht.humidity_id = dt.date_id " +
        "LEFT JOIN station_table st ON st.station_id = dt.date_id " +
        "LEFT JOIN station_info_table sit ON sit.iata = st.station")


    println("######################### Writing output file")
    writeOutput(combinedDf.rdd.map(x => x.mkString("|")))
  }
}
