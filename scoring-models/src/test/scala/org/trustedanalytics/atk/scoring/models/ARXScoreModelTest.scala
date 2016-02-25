/*
// Copyright (c) 2016 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.scoring.models

import org.apache.spark.mllib.ScoringModelTestUtils
import com.cloudera.sparkts.models.ARXModel
import org.scalatest.WordSpec

class ARXScoreModelTest extends WordSpec {

  "ARXScoreModel" should {
    val c = 0
    val coefficients = Array[Double](-1.136026484226831e-08,
      8.637677568908233e-07,
      15238.143039368977,
      -7.993535860373772e-09,
      -5.198597570089805e-07,
      1.5691547009557947e-08,
      7.409621376205488e-08)
    val yMaxLag = 0
    val xMaxLag = 0
    val includesOriginalX = true
    val arxModel = new ARXModel(c, coefficients, yMaxLag, xMaxLag, includesOriginalX)
    val xColumns = List("visitors", "wkends", "seasonality", "incidentRate", "holidayFlag", "postHolidayFlag", "mintemp")
    val arxData = new ARXData(arxModel, xColumns)
    val arxScoreModel = new ARXScoreModel(arxModel, arxData)

    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(arxScoreModel)
    }

    "throw an exception when scoring data with too few columns" in {
      ScoringModelTestUtils.tooFewDataColumnsTest(arxScoreModel, xColumns.size + 1)
    }

    "throw an exception when scoring data with too many columns" in {
      ScoringModelTestUtils.tooManyDataColumnsTest(arxScoreModel, xColumns.size + 1)
    }

    "throw an exception when scoring data with non-numerical records" in {
      ScoringModelTestUtils.invalidDataTest(arxScoreModel, xColumns.size + 1)
    }

    "successfully score a model when valid data is provided" in {
      // input is y = 100, and x values = 465,1,0.006562479,24,1,0,51
      val input = Array[Any](100, 465, 1, 0.006562479, 24, 1, 0, 51)

      // call score model to predict
      val predictions = arxScoreModel.score(input)

      // we should get back just one prediction value
      assert(predictions.length == 1)

      // the actual y value is 100.  check that the prediction is in the general range of this value
      val yPrediction = predictions(0).asInstanceOf[Double]
      assert(yPrediction > 90)
      assert(yPrediction < 110)
    }

  }

}
