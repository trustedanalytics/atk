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

package org.trustedanalytics.atk.scoring.models

import org.scalatest.WordSpec

/**
 * Tests for the ScoringModelUtils functions
 */
class ScoringModelUtilsTest extends WordSpec {

  "ScoringModelUtils.toInt" should {
    "succeed for numerical values" in {
      assert(5 == ScoringModelUtils.toInt(5))
      assert(5 == ScoringModelUtils.toInt(5.0))
      assert(-5 == ScoringModelUtils.toInt(-5))
      assert(-5 == ScoringModelUtils.toInt(-5.0))
      assert(5 == ScoringModelUtils.toInt("5"))
      assert(-5 == ScoringModelUtils.toInt("-5"))
    }

    "throw an exception for invalid float/decimal values" in {
      intercept[IllegalArgumentException] {
        ScoringModelUtils.toInt(null)
      }
      intercept[IllegalArgumentException] {
        ScoringModelUtils.toInt(5.5)
      }
      intercept[IllegalArgumentException] {
        ScoringModelUtils.toInt(-5.5)
      }
    }

    "throw an excpetion for invalid string values" in {
      intercept[NumberFormatException] {
        ScoringModelUtils.toInt("abc")
      }
      intercept[NumberFormatException] {
        ScoringModelUtils.toInt("5a")
      }
      intercept[NumberFormatException] {
        ScoringModelUtils.toInt("7*5")
      }
      intercept[NumberFormatException] {
        ScoringModelUtils.toInt("7.5")
      }
    }
  }

  "ScoringModelUtils.isNumerical" should {
    "return true when passed a numerical value" in {
      assert(true == ScoringModelUtils.isNumeric(5))
      assert(true == ScoringModelUtils.isNumeric(-5))
      assert(true == ScoringModelUtils.isNumeric(5.7))
      assert(true == ScoringModelUtils.isNumeric(-5.7))
      assert(true == ScoringModelUtils.isNumeric("5"))
      assert(true == ScoringModelUtils.isNumeric("-5"))
      assert(true == ScoringModelUtils.isNumeric("24156"))
      assert(true == ScoringModelUtils.isNumeric("-578.2"))
    }

    "return false when passed a non-numeric value" in {
      assert(false == ScoringModelUtils.isNumeric("a5"))
      assert(false == ScoringModelUtils.isNumeric("5a"))
      assert(false == ScoringModelUtils.isNumeric("abc123"))
      assert(false == ScoringModelUtils.isNumeric("5.67*"))
      assert(false == ScoringModelUtils.isNumeric(null))
    }
  }
}
