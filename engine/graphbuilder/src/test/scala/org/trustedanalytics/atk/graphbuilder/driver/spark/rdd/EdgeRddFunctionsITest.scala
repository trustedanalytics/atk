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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GbIdToPhysicalId, Property }
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class EdgeRddFunctionsITest extends TestingSparkContextWordSpec with Matchers {

  "EdgeRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in {

      val edge1 = new GBEdge(None, Property("gbId", 1L), Property("gbId", 2L), "myLabel", Set(Property("key", "value")))
      val edge2 = new GBEdge(None, Property("gbId", 2L), Property("gbId", 3L), "myLabel", Set(Property("key", "value")))
      val edge3 = new GBEdge(None, Property("gbId", 1L), Property("gbId", 2L), "myLabel", Set(Property("key2", "value2")))

      val gbIdToPhysicalId1 = new GbIdToPhysicalId(Property("gbId", 1L), new java.lang.Long(1001L))
      val gbIdToPhysicalId2 = new GbIdToPhysicalId(Property("gbId", 2L), new java.lang.Long(1002L))

      val edges = sparkContext.parallelize(List(edge1, edge2, edge3))
      val gbIdToPhysicalIds = sparkContext.parallelize(List(gbIdToPhysicalId1, gbIdToPhysicalId2))

      val merged = edges.mergeDuplicates()
      merged.count() shouldBe 2

      val biDirectional = edges.biDirectional()
      biDirectional.count() shouldBe 6

      val mergedBiDirectional = biDirectional.mergeDuplicates()
      mergedBiDirectional.count() shouldBe 4

      val filtered = biDirectional.filterEdgesWithoutPhysicalIds()
      filtered.count() shouldBe 0

      val joined = biDirectional.joinWithPhysicalIds(gbIdToPhysicalIds)
      joined.count() shouldBe 4
      joined.filterEdgesWithoutPhysicalIds().count() shouldBe 4

      val vertices = edges.verticesFromEdges()
      vertices.count() shouldBe 6
      vertices.mergeDuplicates().count() shouldBe 3

    }
  }

}
