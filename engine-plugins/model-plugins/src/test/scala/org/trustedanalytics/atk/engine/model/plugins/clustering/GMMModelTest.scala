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
package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class GMMModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  val modelRef = mock[ModelReference]
  val frameRef = mock[FrameReference]
  "GMMModel" should "create a GMMModel" in {
    val vectors = Array(Vectors.dense(2), Vectors.dense(1), Vectors.dense(7),
      Vectors.dense(1), Vectors.dense(9))
    val rdd = sparkContext.parallelize(vectors)
    val trainArgs = GMMTrainArgs(modelRef, frameRef, List("obs1"), List(1), k = 3)

    val gmm = GMMTrainPlugin.initializeGMM(trainArgs)
    val model = gmm.run(rdd)
    model shouldBe a[GaussianMixtureModel]
    model.k shouldBe 3

    val gmmData = new GMMData(model, trainArgs.observationColumns, trainArgs.columnScalings)
    gmmData shouldBe a[GMMData]
  }

  "GMMModel" should "throw an Illegal Argument Exception for empty observation columns name during train" in {
    intercept[IllegalArgumentException] {
      GMMTrainArgs(modelRef, frameRef, observationColumns = List(), columnScalings = List(2.0))
    }
  }

  "GMMMModel" should "throw an Illegal Argument Exception for empty scaling columns name during train" in {
    intercept[IllegalArgumentException] {
      GMMTrainArgs(modelRef, frameRef, observationColumns = List("column1"), columnScalings = List())
    }
  }

  "GMMModel" should "throw an Illegal Argument Exception for unequal observation columns and column scalings during train" in {
    intercept[IllegalArgumentException] {
      GMMTrainArgs(modelRef, frameRef, observationColumns = List("column1", "column2"), columnScalings = List(1.0))
    }
  }
}