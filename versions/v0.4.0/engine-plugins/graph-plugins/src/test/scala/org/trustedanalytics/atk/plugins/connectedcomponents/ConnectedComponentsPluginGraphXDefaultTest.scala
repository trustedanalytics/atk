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

package org.trustedanalytics.atk.plugins.connectedcomponents

import org.scalatest.{ FlatSpec, Matchers }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class ConnectedComponentsPluginGraphXDefaultTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait ConnectedComponentsTest {
    val vertexList: List[Long] = List(1, 2, 3, 5, 6, 7)

    // to save ourselves some pain we enter the directed edge list and then make it bidirectional with a flatmap

    val edgeList: List[(Long, Long)] = List((2.toLong, 6.toLong), (2.toLong, 4.toLong), (4.toLong, 6.toLong),
      (3.toLong, 5.toLong), (3.toLong, 7.toLong), (5.toLong, 7.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

  }

  "connected components" should
    "allocate component IDs according to connectivity equivalence classes" in new ConnectedComponentsTest {

      val rddOfComponentLabeledVertices: RDD[(Long, Long)] =
        ConnectedComponentsGraphXDefault.run(sparkContext.parallelize(vertexList), sparkContext.parallelize(edgeList))

      val vertexIdToComponentMap = rddOfComponentLabeledVertices.collect().toMap

      vertexIdToComponentMap(2) shouldEqual vertexIdToComponentMap(6)
      vertexIdToComponentMap(2) shouldEqual vertexIdToComponentMap(4)

      vertexIdToComponentMap(7) shouldEqual vertexIdToComponentMap(5)
      vertexIdToComponentMap(7) shouldEqual vertexIdToComponentMap(3)

      vertexIdToComponentMap(2) should not be vertexIdToComponentMap(3)
      vertexIdToComponentMap(3) should not be vertexIdToComponentMap(1)

    }
}
