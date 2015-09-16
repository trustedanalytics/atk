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

import java.io.Serializable
import com.thinkaurelius.titan.core.TitanGraph

/**
 * This is the edge manager class.
 */
object EdgeManager extends Serializable {
  /**
   *
   * @param list a  list of iterable edges. If the list has 2 elements, the head element (an edge) of any of the lists can collapse
   * @return true if the edge can collapse; false otherwise
   */
  def canEdgeCollapse(list: Iterable[VertexOutEdges]): Boolean = {
    (null != list) && (list.toArray.length > 1)
  }

  /**
   * Replace a node with associated meta-node in edges.
   * @param edgeList - a list with the following property:
   *                 head - an edge with the meta node as source node
   *                 tail - a list of edges whose destination nodes will need to be replaced
   * @return the edge list (less the head element) with the destination node replaced by head.source
   */
  def replaceWithMetaNode(edgeList: Iterable[GraphClusteringEdge]): Iterable[GraphClusteringEdge] = {

    if (edgeList.toArray.length > 1) {
      var internalEdge: GraphClusteringEdge = null
      for (edge <- edgeList) {
        if (edge.isInternal) {
          internalEdge = edge
        }
      }
      if (null != internalEdge) {
        for (edge <- edgeList) {
          if (edge != internalEdge) {
            edge.distance = edge.distance * edge.destNodeCount
            edge.dest = internalEdge.src
            edge.destNodeCount = internalEdge.srcNodeCount
          }
        }
      }
    }

    edgeList.filter(e => !e.isInternal)
  }

  /**
   * Creates a flat list of edges (to be interpreted as outgoing edges) for a meta-node
   * @param list a list of (lists of) edges. The source node of the head element of each list is the metanode
   * @return a flat list of outgoing edges for metanode
   */
  def createOutgoingEdgesForMetaNode(list: Iterable[VertexOutEdges]): (GraphClusteringEdge, Iterable[GraphClusteringEdge]) = {

    var outgoingEdges: List[GraphClusteringEdge] = List[GraphClusteringEdge]()
    var edge: GraphClusteringEdge = null

    if ((null != list) && list.nonEmpty) {
      for (edgeList <- list) {
        if ((null != edgeList) && (null != edgeList.higherDistanceEdgeList)) {
          outgoingEdges = outgoingEdges ++ edgeList.higherDistanceEdgeList.toList
          edge = edgeList.minDistanceEdge
        }
      }
    }

    (edge, outgoingEdges)
  }

  /**
   * Creates 2 internal edges for a collapsed edge
   * @param edge a collapsed edge
   * @return 2 internal edges replacing the collapsed edge in the graph
   */
  def createInternalEdgesForMetaNode(edge: GraphClusteringEdge,
                                     storage: GraphClusteringStorageInterface,
                                     iteration: Int): (Long, Long, List[GraphClusteringEdge]) = {

    var edges: List[GraphClusteringEdge] = List[GraphClusteringEdge]()

    if (null != edge) {
      val metaNodeVertexId = storage.addVertexAndEdges(
        edge.src,
        edge.dest,
        edge.getTotalNodeCount,
        edge.src.toString + "_" + edge.dest.toString,
        iteration)

      edges = edges :+ GraphClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.src,
        edge.srcNodeCount,
        GraphClusteringConstants.DefaultNodeCount, isInternal = true)
      edges = edges :+ GraphClusteringEdge(metaNodeVertexId,
        edge.getTotalNodeCount,
        edge.dest,
        edge.destNodeCount,
        GraphClusteringConstants.DefaultNodeCount, isInternal = true)

      (metaNodeVertexId, edge.getTotalNodeCount, edges)
    }
    else {
      (GraphClusteringConstants.DefaultVertextId,
        GraphClusteringConstants.DefaultNodeCount,
        edges)
    }

  }

  /**
   * Creates a list of active edges for meta-node
   * @param metaNode
   * @param count
   * @param nonSelectedEdges
   * @return
   */
  def createActiveEdgesForMetaNode(metaNode: Long, count: Long,
                                   nonSelectedEdges: Iterable[GraphClusteringEdge]): List[((Long, Long), GraphClusteringEdge)] = {

    nonSelectedEdges.map(e => ((e.dest, e.destNodeCount),
      GraphClusteringEdge(
        metaNode,
        count,
        e.dest,
        e.destNodeCount,
        e.distance, isInternal = false))).toList
  }
}
