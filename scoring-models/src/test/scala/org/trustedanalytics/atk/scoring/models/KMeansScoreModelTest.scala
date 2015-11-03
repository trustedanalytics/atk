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

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.DenseVector
import org.scalatest.WordSpec

class KMeansScoreModelTest extends WordSpec {

  "KMeansScoreModel" should {
    val kmeansModel = new KMeansModel(Array(new DenseVector(Array(1.2, 2.1)), new DenseVector(Array(3.4, 4.3))))
    var kmeansScoreModel = new KMeansScoreModel(kmeansModel)
    val numRows = 5 // number of rows of data to test with

    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(kmeansScoreModel)
    }

    "throw an exception when scoring data with too few columns" in {
      ScoringModelTestUtils.tooFewDataColumnsTest(kmeansScoreModel, kmeansModel.clusterCenters(0).size, numRows)
    }

    "throw an exception when scoring data with too many columns" in {
      ScoringModelTestUtils.tooManyDataColumnsTest(kmeansScoreModel, kmeansModel.clusterCenters(0).size, numRows)
    }

    "throw an exception when scoring data with non-numerical records" in {
      ScoringModelTestUtils.invalidDataTest(kmeansScoreModel, kmeansModel.clusterCenters(0).size)
    }

    "successfully score a model when float data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(kmeansScoreModel, kmeansModel.clusterCenters(0).size, numRows)
    }

    "successfully score a model when integer data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(kmeansScoreModel, kmeansModel.clusterCenters(0).size, numRows)
    }
  }
}

