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

package org.trustedanalytics.atk.testutils

import breeze.linalg.DenseMatrix
import org.scalatest.Matchers
import org.scalatest.matchers.{ MatchResult, Matcher }
import spray.json._

object MatcherUtils extends Matchers {

  /**
   * Tests if two arrays of Double are equal +- tolerance.
   *
   * <pre class="stHighlight">
   * Array(0.12, 0.25) should  equalWithTolerance(Array(0.122, 0.254), 0.01)
   * </pre>
   */
  def equalWithTolerance(right: Array[Double], tolerance: Double = 1E-6) =
    Matcher { (left: Array[Double]) =>
      MatchResult(
        (left zip right) forall { case (a, b) => a === (b +- tolerance) },
        left.deep.mkString(" ") + " did not equal " + right.deep.mkString(" ") + " with tolerance " + tolerance,
        left.deep.mkString(" ") + " equaled " + right.deep.mkString(" ") + " with tolerance " + tolerance
      )
    }

  /**
   * Tests if two Scala Breeze dense matrices are equal +- tolerance.
   *
   * <pre class="stHighlight">
   * DenseMatrix((1330d, 480d)) should  equalWithTolerance(DenseMatrix((1330.02, 480d.09)), 0.1)
   * </pre>
   */
  def equalWithToleranceMatrix(right: DenseMatrix[Double], tolerance: Double = 1E-6) =
    Matcher { (left: DenseMatrix[Double]) =>
      MatchResult(
        if (left.size === right.size) {
          val results = for {
            i <- 0 until right.rows
            j <- 0 until right.cols
            r = left(i, j) === (right(i, j) +- tolerance)
          } yield r
          results forall (x => x)
        }
        else false,
        left.toString() + " did not equal " + right.toString() + " with tolerance " + tolerance,
        left.toString() + " equaled " + right.toString() + " with tolerance " + tolerance
      )
    }

  /**
   * Get field value from JSON object using key, and convert value to a Scala object
   */
  private def getJsonFieldValue(json: JsValue, key: String): Any = json match {
    case obj: JsObject => {
      val value = obj.fields.get(key).orNull
      value match {
        case x: JsBoolean => x.value
        case x: JsNumber => x.value
        case x: JsString => x.value
        case x => x.toString
      }
    }
    case x => x.toString()
  }

}
