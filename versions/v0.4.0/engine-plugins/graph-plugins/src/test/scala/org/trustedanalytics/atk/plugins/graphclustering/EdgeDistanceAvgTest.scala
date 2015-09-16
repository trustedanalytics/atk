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

package org.trustedanalytics.atk.plugins.graphclustering

import org.scalatest.{ FlatSpec, Matchers }

class EdgeDistanceAvgTest extends FlatSpec with Matchers {

  val emptyEdgeList = List()

  val basicEdgeList: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.3f, false)
  )

  val reversedEdgeList: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 4, 2, 1, 1.0f, false),
    GraphClusteringEdge(2, 1, 1, 1, 1.4f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  "edgeDistance::weightedAvg" should "be 0 for empty lists" in {
    val dist = EdgeDistance.weightedAvg(emptyEdgeList)
    assert(dist == 0)
  }

  "edgeDistance::weightedAvg" should "be non 0" in {
    val dist = EdgeDistance.weightedAvg(basicEdgeList)
    assert(dist == 1.2f)
  }

  "edgeDistance::weightedAvg" should "return non 0 value" in {
    val dist = EdgeDistance.weightedAvg(reversedEdgeList)
    assert(dist == 1.1f)
  }

  "edgeDistance::simpleAvg" should "be 0 for empty lists" in {
    val edge = EdgeDistance.simpleAvg(emptyEdgeList, false)
    assert(edge == null)
  }

  "edgeDistance::simpleAvg" should "be non 0" in {
    val edge = EdgeDistance.simpleAvg(basicEdgeList, false)
    assert(edge.distance == 1.2f)
  }

  "edgeDistance::simpleAvg" should "return non 0 value" in {
    val edge = EdgeDistance.simpleAvg(reversedEdgeList, false)
    assert(edge.distance == 1.2f)
  }
}
