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

package org.trustedanalytics.atk.graphbuilder.elements

import org.scalatest.{ Matchers, WordSpec }

class GBEdgeTest extends WordSpec with Matchers {

  val tailId = new Property("gbId", 10001)
  val headId = new Property("gbId", 10002)
  val label = "myLabel"
  val edge = new GBEdge(None, tailId, headId, label, Set(new Property("key", "value")))

  "Edge" should {

    "be reverse-able" in {
      // invoke method under test
      val reversedEdge = edge.reverse()

      // should be opposite
      edge.headVertexGbId shouldBe reversedEdge.tailVertexGbId
      edge.tailVertexGbId shouldBe reversedEdge.headVertexGbId

      // should be same same
      edge.label shouldBe reversedEdge.label
      edge.properties shouldBe reversedEdge.properties
    }

    "have a unique id made up of the tailId, headId, and label" in {
      edge.id shouldBe (tailId, headId, label)
    }

    "be mergeable" in {
      val edge2 = new GBEdge(None, tailId, headId, label, Set(new Property("otherKey", "otherValue")))

      // invoke method under test
      val merged = edge.merge(edge2)

      merged.properties shouldBe Set(Property("key", "value"), Property("otherKey", "otherValue"))

    }

    "not allow merging of edges with different ids" in {
      val diffId = new Property("gbId", 9999)
      val edge2 = new GBEdge(None, tailId, diffId, label, Set(new Property("otherKey", "otherValue")))

      intercept[IllegalArgumentException] {
        edge.merge(edge2)
      }
    }
  }
}
