package com.ebon


import com.ebon.ArgMap._
import com.ebon.Spark._
import com.typesafe.scalalogging.slf4j.LazyLogging


object Workflow extends LazyLogging{
  def main(args: Array[String]): Unit = {
/*
    val argMap = ArgMap(args)
    val argsL = args.length
    if(argsL < 1) {
      logger.error("Usage: spark-submit --class com.ebon.Workflow <artifact jar> numExtraDays")
      System.exit(1)
    }
*/
    TimeSeries.runTimeSeries()
    IdwInterpolation.runInterpolation()
    sc.stop()
  }
}