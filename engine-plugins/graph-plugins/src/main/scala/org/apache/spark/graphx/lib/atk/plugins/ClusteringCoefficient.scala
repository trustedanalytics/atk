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

package org.apache.spark.graphx.lib.atk.plugins

import org.apache.spark.graphx._
import org.apache.spark.util.collection.OpenHashSet

import scala.reflect.ClassTag

/**
 * Compute the local clustering coefficient of each vertex in a graph.
 *
 *
 * Note that the input graph should have its edges in canonical direction
 * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
 * using [[org.apache.spark.graphx.Graph#partitionBy]].
 *
 * PERFORMANCE/COMPATIBILITY NOTE:  This routine was written to the GraphX API in Spark 1.1
 * Going forward, an upgrade to the Spark 1.2 GraphX API could yield performance gains.
 */
object ClusteringCoefficient {

  type VertexSet = OpenHashSet[Long]

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): (Graph[Double, ED], Double) = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[VertexSet] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new VertexSet(4)
        var i = 0
        while (i < nbrs.length) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
          }
          i += 1
        }
        set
      }
    // join the sets with the graph
    val setGraph: Graph[VertexSet, ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(et: EdgeTriplet[VertexSet, ED]): Iterator[(VertexId, Int)] = {
      assert(et.srcAttr != null, "GraphX, clustering coefficient, edgeFunc: edge source has null adjacency list.")
      assert(et.dstAttr != null, "GraphX, clustering coefficient, edgeFunc: edge source has null adjacency list.")
      val (smallSet, largeSet) = if (et.srcAttr.size < et.dstAttr.size) {
        (et.srcAttr, et.dstAttr)
      }
      else {
        (et.dstAttr, et.srcAttr)
      }
      val iter = smallSet.iterator
      var counter: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != et.srcId && vid != et.dstId && largeSet.contains(vid)) {
          counter += 1
        }
      }
      Iterator((et.srcId, counter), (et.dstId, counter))
    }
    // compute the intersection along edges

    val triangleDoubleCounts: VertexRDD[Int] = setGraph.mapReduceTriplets(edgeFunc, _ + _)

    val degreesChooseTwo: Graph[Long, ED] = setGraph.mapVertices({ case (vid, vertexSet) => chooseTwo(vertexSet.size) })

    val doubleCountOfTriangles: Long =
      triangleDoubleCounts.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, triangleDoubleCount: Int)) => x + triangleDoubleCount.toLong }, _ + _)

    val totalDegreesChooseTwo: Long =
      degreesChooseTwo.vertices.aggregate[Long](0L)({ case (x: Long, (vid: VertexId, degreeChoose2: Long)) => x + degreeChoose2 }, _ + _)

    val globalClusteringCoefficient = if (totalDegreesChooseTwo > 0) {
      (doubleCountOfTriangles / 2.0d) / totalDegreesChooseTwo.toDouble
    }
    else {
      0.0d
    }

    val localClusteringCoefficientGraph = degreesChooseTwo.outerJoinVertices(triangleDoubleCounts) {
      (vid, degreeChoose2, optCounter: Option[Int]) =>
        if (degreeChoose2 == 0L) {
          0.0d
        }
        else {
          val triangleDoubleCount = optCounter.getOrElse(0)
          (triangleDoubleCount.toDouble / 2.0d) / degreeChoose2.toDouble
        }
    }

    (localClusteringCoefficientGraph, globalClusteringCoefficient)

  }

  private def chooseTwo(n: Long): Long = (n * (n - 1)) / 2

}
