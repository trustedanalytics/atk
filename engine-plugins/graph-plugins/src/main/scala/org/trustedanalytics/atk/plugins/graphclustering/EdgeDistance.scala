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

import java.io.Serializable

/**
 * This is the edge distance class.
 */
object EdgeDistance extends Serializable {

  /**
   * Calculates the minimum distance of an edge list
   * @param edgeList a list of active edges
   * @return a vertex distance class (vertex id, min distance edge, non-min distance edges)
   */
  def min(edgeList: Iterable[GraphClusteringEdge]): (GraphClusteringEdge, VertexOutEdges) = {

    var dist: Float = Int.MaxValue
    var edgeWithMinDist: GraphClusteringEdge = null
    var nonMinDistEdges: List[GraphClusteringEdge] = List[GraphClusteringEdge]()

    if ((null != edgeList) && edgeList.nonEmpty) {
      for (edge <- edgeList) {
        if (null != edge) {
          if (edge.distance < dist) {

            // found a smaller distance edge.
            // save it in edgeWithMinDist & adjust the overall min distance
            dist = edge.distance
            if (edgeWithMinDist != null) {
              nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
            }
            edgeWithMinDist = edge
          }
          else if (edge.distance == dist) {
            if (edgeWithMinDist != null) {
              if (edge.src.toString < edgeWithMinDist.src.toString) {

                // found an equal distance edge but with node id smaller.
                // save it in edgeWithMinDist
                nonMinDistEdges = nonMinDistEdges :+ edgeWithMinDist
                edgeWithMinDist = edge
              }
              else {

                // found equal distance edge but with node id higher. Add it to the list of non-selected
                nonMinDistEdges = nonMinDistEdges :+ edge
              }
            }
            else {

              // rare scenario. Found a small distance edge but edgeWithMinDist is not set.
              // set it.
              edgeWithMinDist = edge
            }
          }
          else {

            // found bigger distance edge. Add it to the list of non-selected.
            nonMinDistEdges = nonMinDistEdges :+ edge
          }
        }
      }

      if (null != edgeWithMinDist) {

        // edgeWithMinDist can be null in rare cases. We need to test for null
        if (edgeWithMinDist.dest < edgeWithMinDist.src) {

          // swap the node ids so the smaller node is always source
          swapEdgeInfo(edgeWithMinDist)
        }

        (edgeWithMinDist, VertexOutEdges(edgeWithMinDist, nonMinDistEdges))
      }
      else {
        (null, VertexOutEdges(null, null))
      }
    }
    else {
      (null, VertexOutEdges(null, null))
    }
  }

  /**
   * Sum (edgeDistance * SourceNodeWeight) / Sum (SourceNodeWeight)
   *
   * @param edges a list of active edges
   * @return the average distance, as per formula above
   */
  def weightedAvg(edges: Iterable[GraphClusteringEdge]): Float = {
    var dist: Float = 0
    var nodeCount: Long = 0

    for (e <- edges) {
      dist += (e.distance * e.srcNodeCount)
      nodeCount += e.srcNodeCount
    }

    if (nodeCount > 0) {
      dist = dist / nodeCount
    }

    dist
  }

  /**
   * Sum (edgeDistance) / (Total edges in the Iterable)
   *
   * @param edges a list of active edges
   * @return the head of the input list with the distance adjusted as per formula
   */
  def simpleAvg(edges: Iterable[GraphClusteringEdge], swapInfo: Boolean): GraphClusteringEdge = {
    var dist: Float = 0
    var edgeCount = 0

    for (e <- edges) {
      dist += e.distance
      edgeCount += 1
    }

    if (edgeCount > 1) {
      val head = edges.head
      head.distance = dist / edgeCount

      if (swapInfo) {
        swapEdgeInfo(head)
      }
    }

    if (edges.nonEmpty) {
      edges.head
    }
    else {
      null
    }

  }

  def swapEdgeInfo(edge: GraphClusteringEdge): Unit = {

    val tmpName = edge.src
    val tmpNodeCount = edge.srcNodeCount

    edge.src = edge.dest
    edge.dest = tmpName
    edge.srcNodeCount = edge.destNodeCount
    edge.destNodeCount = tmpNodeCount
  }
}
