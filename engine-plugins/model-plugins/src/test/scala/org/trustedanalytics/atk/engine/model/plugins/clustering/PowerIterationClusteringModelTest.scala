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

import org.apache.spark.mllib.clustering.{ PowerIterationClusteringModel, KMeansModel }
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class PowerIterationClusteringModelTest extends TestingSparkContextFlatSpec with Matchers with MockitoSugar {
  "PowerIterationClusteringModel" should "create a PowerIterationClusteringModel" in {
    val modelRef = mock[ModelReference]
    val frameRef = mock[FrameReference]
    val edgeList: Array[(Long, Long, Double)] = Array((1.toLong, 2.toLong, 1.0), (1.toLong, 2.toLong, 0.3),
      (2.toLong, 3.toLong, 0.3), (3.toLong, 0.toLong, 0.03), (0.toLong, 5.toLong, 0.01), (5.toLong, 4.toLong, 0.3),
      (5.toLong, 6.toLong, 1.0), (4.toLong, 6.toLong, 0.3))
    val rdd = sparkContext.parallelize(edgeList)
    val trainArgs = PowerIterationClusteringArgs(modelRef, frameRef, "Source", "Destination", "Distance", k = 3)

    val pic = PowerIterationClusteringPlugin.initializePIC(trainArgs)
    val model = pic.run(rdd)
    model shouldBe a[PowerIterationClusteringModel]

  }

  "PowerIterationClusteringModel" should "thow an IllegalArgumentException for empty source column name during run" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "", destinationColumn = "Destination", similarityColumn = "Distance", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "thow an IllegalArgumentException for empty destination column name during run" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "", similarityColumn = "Distance", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "throw an IllegalArgumentException for empty similarity columns name during run" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "Destination", similarityColumn = "", k = 3)
    }
  }

  "PowerIterationClusteringModel" should "throw an IllegalArgumentException for any incorrect value of k during run" in {
    intercept[IllegalArgumentException] {

      val modelRef = mock[ModelReference]
      val frameRef = mock[FrameReference]

      PowerIterationClusteringArgs(modelRef, frameRef, sourceColumn = "Source", destinationColumn = "Destination", similarityColumn = "Similarity", k = 0)
    }
  }
}

