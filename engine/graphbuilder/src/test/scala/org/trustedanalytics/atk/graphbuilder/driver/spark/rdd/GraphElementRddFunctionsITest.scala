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

package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, _ }
import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class GraphElementRddFunctionsITest extends TestingSparkContextWordSpec with Matchers {

  "GraphElementRDDFunctions" should {

    // A lot of tests are being grouped together here because it
    // is somewhat expensive to spin up a testing SparkContext
    "pass integration test" in {

      val edge1 = new GBEdge(None, Property("gbId", 1L), Property("gbId", 2L), "myLabel", Set(Property("key", "value")))
      val edge2 = new GBEdge(None, Property("gbId", 2L), Property("gbId", 3L), "myLabel", Set(Property("key", "value")))

      val vertex = new GBVertex(Property("gbId", 2L), Set.empty[Property])

      val graphElements = sparkContext.parallelize(List[GraphElement](edge1, edge2, vertex))

      graphElements.filterEdges().count() shouldBe 2
      graphElements.filterVertices().count() shouldBe 1
    }
  }
}
