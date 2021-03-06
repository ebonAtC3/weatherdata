package com.ebon

import scala.util.{Failure, Success, Try}
import com.ebon.Utils._

object DataValidation {

  def failure(m: String) = Failure(new Exception(m))

  def validateNumber(v: String, stringToNumber: String => Any): Option[String] =  {
    val vCleaned     = numberCleaner(v)
    def typedNumber = stringToNumber(vCleaned)
    Try(typedNumber) match {
      case Success(_) => Some(vCleaned)
      case Failure(_) => None
    }
  }

  def validateDouble(v: String) ={
    validateNumber(v, x => x.toDouble)
  }
}