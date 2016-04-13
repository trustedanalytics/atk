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

    // [#]  y      visitors  wkends  seasonality  incidentRate  holidayFlag  postHolidayFlag  mintemp
    //===============================================================================================
    // [0]  100.0     465.0     1.0  0.006562479          24.0          1.0              0.0     51.0
    // [1]   98.0     453.0     1.0   0.00643123          24.0          0.0              1.0     54.0
    // [2]  102.0     472.0     0.0  0.006693729          25.0          0.0              0.0     49.0
    // [3]   98.0     454.0     0.0   0.00643123          25.0          0.0              0.0     46.0
    // [4]  112.0     432.0     0.0  0.007349977          25.0          0.0              0.0     42.0
    var input = new Array[Any](2)
    input(0) = List[Double](100, 98, 102, 98, 112)
    input(1) = List[Double](465.0, 453.0, 472.0, 454.0, 432.0,
      1.0, 1.0, 0.0, 0.0, 0.0,
      0.006562479, 0.00643123, 0.006693729, 0.00643123, 0.007349977,
      24.0, 24.0, 25.0, 25.0, 25.0,
      1.0, 0.0, 0.0, 0.0, 0.0,
      0.0, 1.0, 0.0, 0.0, 0.0,
      51.0, 54.0, 49.0, 46.0, 42.0)

    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(arxScoreModel)
    }

    "throw an exception when given invalid parameters" in {
      // ARX score expects an array of y values and an array of x values
      intercept[IllegalArgumentException] {
        arxScoreModel.score(Array[Any](5.0, 10, 7.1))
      }
    }

    "throw an exception when given too few x values" in {
      var input = new Array[Any](2)
      input(0) = Array[Double](100, 98, 102, 98, 112)
      input(1) = Array[Double](465.0, 1.0)

      intercept[IllegalArgumentException] {
        arxScoreModel.score(input)
      }
    }

    "successfully score a model when valid data is provided" in {
      // call score model and get the last item in the output array
      val output = arxScoreModel.score(input)
      assert(output(output.length - 1).isInstanceOf[Array[Double]])
      val predictions = output(output.length - 1).asInstanceOf[Array[Double]].toList
      assert(predictions.length == 5) // we should get one y prediction per row
      assert(predictions.sameElements(List[Double](99.99999234330198, 98.00000220169095, 101.99999803760333, 98.00000071010813, 111.99999886664024)))
    }

    "successfully score a model with lag values specified" in {
      // Coefficients when using an x and y lag of 1
      val coeffWithLag = Array[Double](-0.04170711405501422,
        -2.774914598250982e-08,
        -1.7345090462674217e-06,
        635.539347392918,
        -2.9524612183525024e-08,
        -42204874328.51074,
        2.38013965984125e-06,
        -6.428834547691361e-08,
        1.143198952375539e-08,
        1.5028156352801287e-06,
        15238.142968296446,
        3.627119251425612e-08,
        -5.299360567706624e-07,
        42204874328.51075,
        1.336808143003387e-07)

      val arxModelWithLag = new ARXModel(c, coeffWithLag, 1, 1, includesOriginalX)
      val arxDataWithLag = new ARXData(arxModelWithLag, xColumns)
      val arxScoreModelWithLag = new ARXScoreModel(arxModelWithLag, arxData)
      val output = arxScoreModelWithLag.score(input)
      assert(output(output.length - 1).isInstanceOf[Array[Double]])
      val predictions = output(output.length - 1).asInstanceOf[Array[Double]].toList
      assert(predictions.length == 4) // we should get one y prediction per row, minus the lag
      assert(predictions.sameElements(List[Double](98.00000721876397, 101.99999878091927, 97.99999998905012, 111.99999804593499)))
    }

  }

}
