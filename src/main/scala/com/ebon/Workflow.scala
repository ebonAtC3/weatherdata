package com.ebon


import com.ebon.Spark._
import com.typesafe.scalalogging.slf4j.LazyLogging


object Workflow extends LazyLogging{
  def main(args: Array[String]): Unit = {
    IdwInterpolation.runInterpolation() // Not used due to scope reduction
    TimeSeries.runTimeSeries()
    sc.stop()
  }
}