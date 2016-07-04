package com.ebon


import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Spark {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var conf = new SparkConf().setAppName("weatherdata")
  var sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
}



