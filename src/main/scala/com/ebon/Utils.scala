package com.ebon


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time._


object Utils {
  def average(doubleList: List[Double]): Double = {
    doubleList.foldLeft(0.0)(_+_) / doubleList.length
  }

  def truncDouble(double: Double): Double = {
    double - (double % 0.1)
  }

  def mergePartitions(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def calculateDateRange(from: DateTime, until: DateTime, extraDays: Int): Seq[DateTime] = {
    val to = until.plusDays(extraDays)
    val numberOfDays = Days.daysBetween(from, to).getDays()
        for (f <- 0 to numberOfDays) yield from.plusDays(f)
  }

  def weatherConditions(rainFall: List[Double], temp: List[Double]): List[String] = {
    val wConditions = List()
    wConditions
  }
}
