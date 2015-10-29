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


package org.trustedanalytics.atk.graphbuilder.elements

import org.scalatest.{ WordSpec, Matchers }

class GBVertexTest extends WordSpec with Matchers {

  val gbId = new Property("gbId", 10001)
  val vertex = new GBVertex(gbId, Set(new Property("key", "value")))

  "Vertex" should {
    "have a unique id that is the gbId" in {
      vertex.id shouldBe gbId
    }

    "be mergeable with another vertex" in {
      val vertex2 = new GBVertex(gbId, Set(new Property("anotherKey", "anotherValue")))

      // invoke method under test
      val merged = vertex.merge(vertex2)

      merged.gbId shouldBe gbId
      merged.properties shouldEqual Set(Property("key", "value"), Property("anotherKey", "anotherValue"))
    }

    "not allow null gbIds" in {
      intercept[IllegalArgumentException] {
        new GBVertex(null, Set.empty[Property])
      }
    }

    "not allow merging of vertices with different ids" in {
      val diffId = new Property("gbId", 10002)
      val vertex2 = new GBVertex(diffId, Set(new Property("anotherKey", "anotherValue")))

      intercept[IllegalArgumentException] {
        // invoke method under test
        vertex.merge(vertex2)
      }
    }
  }

}
