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

import org.scalatest.{ FlatSpec, Matchers }

class EdgeManagerTest extends FlatSpec with Matchers {

  val metaNodeId = 10
  val metaNodeCount = 100

  val basicEdgeList: List[GraphClusteringEdge] = List(
    GraphClusteringEdge(1, 1, 2, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.2f, false)
  )

  val minEdge = GraphClusteringEdge(1, 1, 2, 1, 1.1f, false)
  val nonCollapsableEdgeList: List[VertexOutEdges] = List(
    VertexOutEdges(minEdge, basicEdgeList)
  )
  val outgoingEdgeList: List[VertexOutEdges] = List(
    VertexOutEdges(minEdge, basicEdgeList),
    VertexOutEdges(minEdge, basicEdgeList)
  )

  val metaNodeEdgeList = List(
    GraphClusteringEdge(1, 1, 2, 1, 1.1f, false),
    GraphClusteringEdge(2, 1, 3, 1, 1.2f, false),
    GraphClusteringEdge(metaNodeId, metaNodeCount, 3, 1, 1.2f, true)
  )

  "edgeManager::createActiveEdgesForMetaNode" should "return valid active edge list" in {
    val result: List[((Long, Long), GraphClusteringEdge)] =
      EdgeManager.createActiveEdgesForMetaNode(metaNodeId, metaNodeCount, basicEdgeList)
    result.foreach(item =>
      {
        val ((id: Long, count: Long), edge: GraphClusteringEdge) = item
        assert(edge.src == metaNodeId)
        assert(edge.srcNodeCount == metaNodeCount)
      })
  }

  "edgeManager::createOutgoingEdgesForMetaNode" should "return valid outgoing edge list" in {
    val (edge, outgoingEdges): (GraphClusteringEdge, Iterable[GraphClusteringEdge]) =
      EdgeManager.createOutgoingEdgesForMetaNode(outgoingEdgeList)

    assert(edge == minEdge)
    assert(outgoingEdges.size == outgoingEdgeList.size * basicEdgeList.size)
  }

  "edgeManager::replaceWithMetaNode" should "return the input edge list if no internal edge is present" in {
    val edges: Iterable[GraphClusteringEdge] =
      EdgeManager.replaceWithMetaNode(basicEdgeList)

    assert(edges == basicEdgeList)
  }

  "edgeManager::replaceWithMetaNode" should "return valid edge list with meta-node inserted" in {
    val edges: Iterable[GraphClusteringEdge] =
      EdgeManager.replaceWithMetaNode(metaNodeEdgeList)

    assert(edges.size == 2)
    edges.foreach(item =>
      {
        assert(item.dest == metaNodeId)
        assert(item.destNodeCount == metaNodeCount)
        assert(!item.isInternal)
      })
  }

  "edgeManager::canEdgeCollapse" should "return true" in {
    assert(EdgeManager.canEdgeCollapse(outgoingEdgeList))
  }

  "edgeManager::canEdgeCollapse" should "return false" in {
    assert(!EdgeManager.canEdgeCollapse(nonCollapsableEdgeList))
  }
}
