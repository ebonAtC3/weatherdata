package com.ebon


import com.ebon.DataValidation._
import org.scalatest.{FlatSpec, Matchers}


class DataValidationSpecs extends FlatSpec with Matchers {

  "validateDouble" should "return a double if it is a doubles or return nothing" in {
    validateDouble("123.00") should be(Some("123.00"))
    validateDouble("123,456") should be(Some("123456"))
    validateDouble(" 12 3,4 56 ") should be(Some("123456"))
    validateDouble("123a") should be(None)
    validateDouble("") should be (None)
  }

  "validateNumber" should "return a number if it is a number or return nothing" in {
    validateNumber("     123.00", x => x.toDouble) should be(Some("123.00"))
    validateNumber("123,456", x => x.toInt) should be(Some("123456"))
    validateNumber(" 12 3,4 56 ", x => x.toFloat) should be(Some("123456"))
    validateNumber("9876", x => x.toInt) should be(Some("9876"))
    validateNumber("9,900.00     ", x => x.toDouble) should be(Some("9900.00"))
    validateDouble("123a") should be(None)
    validateDouble("") should be (None)
  }
}
