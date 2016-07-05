package com.ebon


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.joda.time._


object Utils {

  def numberCleaner(v: String) = {
    v.replaceAll( """[,\s]""", "")
  }

  def average(doubleList: List[Double]): Option[Double] = {
     doubleList.isEmpty match {
       case true => None
       case _ => Some(doubleList.foldLeft(0.0)(_+_) / doubleList.length)
     }
  }

  def calculateDateRange(from: DateTime, until: DateTime, extraDays: Int): Seq[DateTime] = {
    val to = until.plusDays(extraDays)
    val numberOfDays = Days.daysBetween(from, to).getDays()
        for (f <- 0 to numberOfDays) yield from.plusDays(f)
  }

  // IO Utils
  def mergePartitions(srcPath: String, dstPath: String) =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  // UDFs for SQL use. Not Option{} allowed
  def roundDouble(double: Double): Double = {
    Math.round(double*100.0)/100.0
  }
  def doubleToInt(double: Double): Int ={
    double.toInt
  }
}
