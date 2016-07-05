package com.ebon


import org.scalatest.{FlatSpec, Matchers}
import com.ebon.Utils._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


class UtilsSpecs extends FlatSpec with Matchers {
  "numberCleaner" should "return a potential valid number passed as a string without any commas and spaces" in {
    numberCleaner("abc,123   ") should be ("abc123")
    numberCleaner("     456,123   ") should be ("456123")
    numberCleaner("123.00") should be ("123.00")
    numberCleaner("    ") should be ("")
  }

  "average" should "return the average value of a list double" in {
    average(List(10.00,20.00)) should be (Some(15.00))
    average(List()) should be (None)
  }

  "calculateDateRange" should "given a number of extra days return a sequence of datetime including these extra days" in{
    val from = DateTime.parse("2016-04-01 00:00:00", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val to = DateTime.parse("2016-04-02 00:00:00", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val extra = DateTime.parse("2016-04-03 00:00:00", DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
    val dateSeq = Seq(from, to)
    val numExtraDays = 1
    val result = Seq(from, to, extra)

    calculateDateRange(from, to, numExtraDays) should be (result)
  }

  "doubleToInt" should "convert a double as an int" in {
    doubleToInt(123.456) should be (123)
    doubleToInt(456) should be (456)
  }

  "roundDouble" should "return a double with two decimals" in{
    roundDouble(12.1234567890) should be (12.12)
    roundDouble(12) should be (12)
  }
}
