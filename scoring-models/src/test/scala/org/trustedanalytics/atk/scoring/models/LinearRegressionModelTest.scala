/*
// Copyright (c) 2015 Intel Corporation 
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

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.scalatest.time.{Millis, Seconds, Span}

class LinearRegressionModelTest extends ScoringModelTest {
  val weights = new DenseVector(Array(2,3))
  val intercept = 4
  val linearRegressionModel = new LinearRegressionModel(weights, intercept)
  var linearRegressionScoreModel = new LinearRegressionScoreModel(linearRegressionModel)
  val numRows = 5    // number of rows of data to test with

  "LinearRegressionModel" should "throw an exception when attempting to score null data" in {
    nullDataTest(linearRegressionScoreModel)
  }

  it should "throw an exception when scoring data with too few columns" in {
    tooFewDataColumnsTest(linearRegressionScoreModel, weights.size, numRows)
  }

  it should "throw an exception when scoring data with too many columns" in {
    tooManyDataColumnsTest(linearRegressionScoreModel, weights.size, numRows)
  }

  it should "throw an exception when scoring data with non-numerical records" in {
    invalidDataTest(linearRegressionScoreModel, weights.size)
  }

  it should "successfully score a model when float data is provided" in {
    successfulModelScoringFloatTest(linearRegressionScoreModel, weights.size, numRows)
  }

  it should "successfully score a model when integer data is provided" in {
    successfulModelScoringFloatTest(linearRegressionScoreModel, weights.size, numRows)
  }

  it should "return results that match the expected results based on the specified weights and intercept" in {
    val data = Seq(Array("1","1.7"), Array("2", "3"), Array("5.32", "0"))
    val testWeights = new DenseVector(Array(2,3))
    val testIntercept = 4
    val testModel = new LinearRegressionModel(testWeights, testIntercept)
    val score = linearRegressionScoreModel.score(data)

    whenReady(score, timeout(Span(scoreTimeoutSeconds, Seconds)), interval(Span(scoreIntervalMillis, Millis))) { result =>
      assert(score.isCompleted)
      assert(data.size == result.length)

      for (i <- data.indices)
      {
        val x1 = data(i)(0).toDouble
        val x2 = data(i)(1).toDouble
        val y = (x1*testWeights.apply(0)) + (x2*testWeights.apply(1)) + testIntercept + 1
        assert(result(i).equals(y))
      }
    }

  }
}
