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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class KMeansModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {

  "KMeansModel" should "create a KMeansModel" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val vectors = Array(Vectors.dense(2), Vectors.dense(1), Vectors.dense(7),
      Vectors.dense(1), Vectors.dense(9))
    val rdd = sparkContext.parallelize(vectors)
    val trainArgs = KMeansTrainArgs(modelRef, frameRef, List("obs1"), List(1))

    val kMeans = KMeansTrainPlugin.initializeKmeans(trainArgs)
    val model = kMeans.run(rdd)
    model shouldBe a[KMeansModel]

    val kmeansData = new KMeansData(model, trainArgs.observationColumns, trainArgs.columnScalings)
    kmeansData shouldBe a[KMeansData]
  }

  "KMeansModel" should "thow an IllegalArgumentException for empty observationColumns during train" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      KMeansTrainArgs(modelRef, frameRef, List(), List(1))
    }
  }

  "KMeansModel" should "thow an IllegalArgumentException for empty columnScalings during train" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      KMeansTrainArgs(modelRef, frameRef, List("obs1", "obs2"), List())
    }
  }
}