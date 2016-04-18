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

import com.cloudera.sparkts.models.ARIMAModel
import org.scalatest.WordSpec
import org.apache.spark.mllib.ScoringModelTestUtils

class ARIMAScoreModelTest extends WordSpec {
  val p = 1
  val d = 0
  val q = 0
  val coefficients = Array[Double](9.864444620964322, 0.2848511106449633, 0.47346114378593795)
  val hasIntercept = true
  val arimaModel = new ARIMAModel(p, d, q, coefficients, hasIntercept)
  val arimaData = new ARIMAData(arimaModel)
  val arimaScoreModel = new ARIMAScoreModel(arimaModel, arimaData)

  "ARIMAScoreModel" should {
    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(arimaScoreModel)
    }

    "throw an exception when data is passed in an invalid format" in {
      // ARIMA score model only expects an list of doubles and an integer for the number of future periods
      intercept[IllegalArgumentException] {
        arimaScoreModel.score(Array[Any](12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 5))
      }
      intercept[IllegalArgumentException] {
        arimaScoreModel.score(Array[Any](List[Double](12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628), 5.5))
      }
    }

    "score a model when valid data is passed as an array and integer (v2 format)" in {
      val goldenValues = List[Any](12.88969427, 13.54964408, 13.8432745, 12.13843611, 12.81156092, 14.2499628, 15.12102595)
      val future = 1 // number of future periods to predict, beyond the length of the golden time series

      // Input array contains the golden values and number of future periods
      var input = new Array[Any](2)
      input(0) = goldenValues
      input(1) = future

      // Call score model to predict
      val output = arimaScoreModel.score(input)
      assert(output.length == (input.length + 1))

      // grab the array of predictions from the last element in the output
      assert(output(output.length - 1).isInstanceOf[Array[Double]])
      val predictions = output(output.length - 1).asInstanceOf[Array[Double]].toList
      assert(predictions.length == (goldenValues.length + future))
      assert(predictions.sameElements(List(12.674342627141744, 13.536088349647843, 13.724075785996275,
        13.807716737252422, 13.322091628390751, 13.51383197812193, 13.923562351193734, 13.830586820836254)))
    }
  }
}
