/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.trustedanalytics.atk.domain.frame

trait Missing[T] {
  def setting(): String

  def evaluate(value: Any): T = {
    if (value != null)
      return value.asInstanceOf[T]
    else
      return null.asInstanceOf[T]
  }
}

case class MissingIgnore[T]() extends Missing[T] {
  override def setting(): String = {
    "ignore"
  }
}

case class MissingImmediate[T](imm: T) extends Missing[T] {
  override def setting(): String = {
    imm.toString
  }

  override def evaluate(value: Any): T = {
    if (value != null)
      return value.asInstanceOf[T]
    else
      return imm
  }
}

/*
case class Missings(missings: Any) {
  val missingKeywordOptions = Array("ignore")
  var missingImmediateValue = 0.0

  if (missings != null && missingKeywordOptions.contains(missings.toString) == false) {
    // If the parameter is not one of our keywords, ensure that it's a numerical value
    require((allCatch opt missings.toString.toDouble).isDefined, "The \"missing\" value must be a valid keyword (i.e. " + missingKeywordOptions.mkString(", ") + ") or a numerical value.")

    missingImmediateValue = missings.toString.toDouble
  }

  /**
   * Evaluates the specified value based on behavior specified for replacing missing values.  If the specified value
   * is not missing, the original value is returned.  If the specified value is missing, the value returned is
   * determined by the missing setting.
   *
   * @param value Specify the value to evaluate.
   * @return Evaluated value, as a double.  If the value is null, and missing is set to "ignore", NaN is returned.
   */
  def evaluateValueAsDouble(value: Any): Double = {
    var retVal = Double.NaN

    if (value != null)
      retVal = DataTypes.toDouble(value)
    else if (value == null && missings != "ignore") {
      retVal = missingImmediateValue
    }

    return retVal
  }
}
*/ 