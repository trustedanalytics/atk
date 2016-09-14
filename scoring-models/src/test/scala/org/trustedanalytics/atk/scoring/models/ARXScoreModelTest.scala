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
    /**
     * Tests using snippets of air quality data from: https://archive.ics.uci.edu/ml/datasets/Air+Quality.
     *
     * Lichman, M. (2013). UCI Machine Learning Repository [http://archive.ics.uci.edu/ml].
     * Irvine, CA: University of California, School of Information and Computer Science.
     */
    val c = 0
    val coefficients = Array[Double](0.005567992923907625,
      -0.010969068059453009,
      0.012556586798371176,
      -0.39792503380811506,
      0.04289162879826746,
      -0.012253952164677924,
      0.01192148525581035,
      0.014100699808650077,
      -0.021091473795935345,
      0.007622676727420039)
    val yMaxLag = 0
    val xMaxLag = 0
    val includesOriginalX = true
    val arxModel = new ARXModel(c, coefficients, yMaxLag, xMaxLag, includesOriginalX)
    val xColumns = List("CO_GT", "PT08_S1_CO", "NMHC_GT", "C6H6_GT", "PT08_S2_NMHC", "NOx_GT", "PT08_S3_NOx", "NO2_GT", "PT08_S4_NO2", "PT08_S5_O3_")
    val arxData = new ARXData(arxModel, xColumns)
    val arxScoreModel = new ARXScoreModel(arxModel, arxData)

    //  [#]  Date        Time      CO_GT  PT08_S1_CO  NMHC_GT  C6H6_GT  PT08_S2_NMHC
    //============================================================================
    //  [0]  10/03/2004  18.00.00    2.6        1360      150     11.9          1046
    //  [1]  10/03/2004  19.00.00    2.0        1292      112      9.4           955
    //  [2]  10/03/2004  20.00.00    2.2        1402       88      9.0           939
    //  [3]  10/03/2004  21.00.00    2.2        1376       80      9.2           948
    //  [4]  10/03/2004  22.00.00    1.6        1272       51      6.5           836
    //<BLANKLINE>
    //  [#]  NOx_GT  PT08_S3_NOx  NO2_GT  PT08_S4_NO2  PT08_S5_O3_  T     RH    AH
    //  ==============================================================================
    //  [0]     166         1056     113         1692         1268  13.6  48.9  0.7578
    //  [1]     103         1174      92         1559          972  13.3  47.7  0.7255
    //  [2]     131         1140     114         1555         1074  11.9  54.0  0.7502
    //  [3]     172         1092     122         1584         1203  11.0  60.0  0.7867
    //  [4]     131         1205     116         1490         1110  11.2  59.6  0.7888

    var input = new Array[Any](2)
    input(0) = List[Double](13.6, 13.3, 11.9, 11.0, 11.2)
    input(1) = List[Double](2.6, 2.0, 2.2, 2.2, 1.6,
      1360, 1292, 1402, 1376, 1272,
      150, 112, 88, 80, 51,
      11.9, 9.4, 9.0, 9.2, 6.5,
      1046, 955, 939, 948, 836,
      166, 103, 131, 172, 131,
      1056, 1174, 1140, 1092, 1205,
      113, 92, 114, 122, 116,
      1692, 1559, 1555, 1584, 1490,
      1268, 972, 1074, 1203, 1110)

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
      assert(predictions.sameElements(List[Double](13.2364599379956, 13.02501308994565, 11.414728229443007, 11.315745782218174, 11.398207488344449)))
    }

    "successfully score a model with lag values specified" in {
      // Coefficients when using an x and y lag of 1
      val coeffWithLag = Array[Double](0.9881422741282173,
        -0.004072936796109567,
        0.008612711321998763,
        0.0007219897811913594,
        0.4591790086176684,
        -0.052997815147511014,
        0.017571292898042712,
        -0.009805576804183673,
        -0.04407984827826915,
        0.026804775025487535,
        0.00026476585578707963,
        0.03216813521172987,
        0.011204577049556986,
        0.01967677582534916,
        -0.8264926308110153,
        0.01866078611542236,
        0.0034239209773078622,
        0.0015717072750842942,
        -0.0034352968450718146,
        -0.01538437870160126,
        3.719728134292509e-05)

      val arxModelWithLag = new ARXModel(c, coeffWithLag, 1, 1, includesOriginalX)
      val arxDataWithLag = new ARXData(arxModelWithLag, xColumns)
      val arxScoreModelWithLag = new ARXScoreModel(arxModelWithLag, arxData)
      val output = arxScoreModelWithLag.score(input)
      assert(output(output.length - 1).isInstanceOf[Array[Double]])
      val predictions = output(output.length - 1).asInstanceOf[Array[Double]].toList
      assert(predictions.length == 4) // we should get one y prediction per row, minus the lag
      assert(predictions.sameElements(List[Double](13.278950799529294, 11.895832885423557, 11.031422453460186, 11.064013498766611)))
    }

  }

}
