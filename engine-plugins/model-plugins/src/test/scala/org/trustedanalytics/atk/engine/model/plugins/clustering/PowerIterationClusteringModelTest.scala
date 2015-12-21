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

import org.apache.spark.mllib.clustering.PowerIterationClustering.Assignment
import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class PowerIterationClusteringModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  val modelRef = mock[ModelReference]
  val frameRef = mock[FrameReference]
  "PowerIterationClusteringModel" should "create a PowerIterationClusteringModel" in {

    val edgeList: Array[(Long, Long, Double)] = Array((1L, 2L, 1.0), (1L, 2L, 0.3),
      (2L, 3L, 0.3), (3L, 0L, 0.03), (0L, 5L, 0.01), (5L, 4L, 0.3), (5L, 6L, 1.0), (4L, 6L, 0.3))
    val rdd = sparkContext.parallelize(edgeList)
    val trainArgs = PowerIterationClusteringArgs(modelRef, frameRef, "Source", "Destination", "Distance", k = 3)

    val pic = PowerIterationClusteringPlugin.initializePIC(trainArgs)
    val model = pic.run(rdd)

    model shouldBe a[PowerIterationClusteringModel]
    model.k shouldBe 3
    val assignmentsArray = model.assignments.collect()
    assignmentsArray(0) shouldBe a[Assignment]
    assignmentsArray(0).id shouldBe 4L

  }

  "PowerIterationClusteringModel" should "thow an IllegalArgumentException for empty source column name during run" in {
    intercept[IllegalArgumentException] {
      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "", destinationColumn = "Destination", similarityColumn = "Distance", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "thow an IllegalArgumentException for empty destination column name during run" in {
    intercept[IllegalArgumentException] {
      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "", similarityColumn = "Distance", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "throw an IllegalArgumentException for empty similarity columns name during run" in {
    intercept[IllegalArgumentException] {
      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "Destination", similarityColumn = "", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "throw an IllegalArgumentException for any incorrect value of k during run" in {
    intercept[IllegalArgumentException] {
      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "Destination", similarityColumn = "Similarity", k = 0)
    }
  }
}

