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

package org.trustedanalytics.atk.plugins.graphclustering

import org.scalatest.{ Matchers, FlatSpec }

class EdgeDistanceMinTest extends FlatSpec with Matchers {

  val nullEdgeList = null

  val emptyEdgeList = List()

  val basicEdgeList: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  val reversedEdgeList: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 1, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  "edgeDistance::min" should "return null for null inputs" in {
    val (minEdge, list) = EdgeDistance.min(nullEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null, null))
  }

  "edgeDistance::min" should "return null for empty lists" in {
    val (minEdge, list) = EdgeDistance.min(emptyEdgeList)
    assert(minEdge == null)
    assert(list == VertexOutEdges(null, null))
  }

  "edgeDistance::min" should "return non null edge" in {
    val (minEdge, list) = EdgeDistance.min(basicEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)
  }

  "edgeDistance::min" should "return non null edges" in {
    val (minEdge, list) = EdgeDistance.min(reversedEdgeList)
    assert(minEdge.src == 1)
    assert(minEdge.dest == 2)
  }
}
