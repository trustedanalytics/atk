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

package org.apache.spark.ml.regression

import org.apache.spark.mllib.ScoringModelTestUtils
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.WordSpec
import org.trustedanalytics.atk.scoring.models.LinearRegressionData

class LinearRegressionModelTest extends WordSpec {
  val uid = "Id"
  val weights = new DenseVector(Array(2, 3))
  val intercept = 4
  val obsCols = List("a", "b", "c")
  val labelCol = "d"
  val linearRegressionModel = new LinearRegressionModel(uid, weights, intercept)
  val linearRegressionData = new LinearRegressionData(linearRegressionModel, obsCols, labelCol)
  var linearRegressionScoreModel = new LinearRegressionScoreModel(linearRegressionData)
  val numObsCols = obsCols.length

  "LinearRegressionModel" should {
    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(linearRegressionScoreModel)
    }

    "throw an exception when scoring data with too few columns" in {
      ScoringModelTestUtils.tooFewDataColumnsTest(linearRegressionScoreModel, weights.size)
    }

    "throw an exception when scoring data with too many columns" in {
      ScoringModelTestUtils.tooManyDataColumnsTest(linearRegressionScoreModel, weights.size)
    }

    "throw an exception when scoring data with non-numerical records" in {
      ScoringModelTestUtils.invalidDataTest(linearRegressionScoreModel, weights.size)
    }

    "successfully score a model when float data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(linearRegressionScoreModel, weights.size)
    }

    "successfully score a model when integer data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(linearRegressionScoreModel, weights.size)
    }

    "successfully return the observation columns used for training the model" in {
      ScoringModelTestUtils.successfulInputTest(linearRegressionScoreModel, numObsCols)
    }

    "successfully return the observation columns used for training the model along with score" in {
      ScoringModelTestUtils.successfulOutputTest(linearRegressionScoreModel, numObsCols)
    }

  }
}
