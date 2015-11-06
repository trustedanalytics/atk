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

package org.trustedanalytics.atk.plugins.idassigner

import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import scala.reflect.ClassTag

/**
 * Renames the vertices of a graph from some arbitrary type T (that provides a ClassTag for Spark's benefit)
 * to Long IDs.
 *
 * @tparam T type of the vertex IDs in the incoming graph
 */

class GraphIDAssigner[T: ClassTag]() extends Serializable {

  /**
   * Rename the vertices of the incoming graph from IDs of type T to Longs
   * @param inVertices vertex list of incoming graph
   * @param inEdges edges list of incoming graph
   * @return GraphIDAssignerOutput
   */
  def run(inVertices: RDD[T], inEdges: RDD[(T, T)]) = {

    val verticesGroupedByHashCodes = inVertices.map(v => (v.hashCode(), v)).groupBy(_._1).map(p => p._2)

    val hashGroupsWithPositions = verticesGroupedByHashCodes.flatMap(seq => seq.zip(1 to seq.size))

    val newIdsToOld = hashGroupsWithPositions.map(
      { case ((hashCode, vertex), bucketPosition) => ((hashCode.toLong << 32) + bucketPosition.toLong, vertex) })

    val oldIdsToNew = newIdsToOld.map({ case (newId, oldId) => (oldId, newId) })

    val newVertices = newIdsToOld.map({ case (newId, _) => newId })

    val edgesGroupedWithNewIdsOfSources = inEdges.cogroup(oldIdsToNew).map(_._2)

    // the id list is always a singleton list because there is one new ID for each incoming vertex
    // this keeps the serialization of the closure relatively small

    val edgesWithSourcesRenamed = edgesGroupedWithNewIdsOfSources.
      flatMap({ case (dstList, srcIdList) => dstList.flatMap(dst => srcIdList.map(srcId => (srcId, dst))) })

    val partlyRenamedEdgesGroupedWithNewIdsOfDestinations = edgesWithSourcesRenamed
      .map({ case (srcWithNewId, dstWithOldId) => (dstWithOldId, srcWithNewId) })
      .cogroup(oldIdsToNew).map(_._2)

    // the id list is always a singleton list because there is one new ID for each incoming vertex
    // this keeps the serialization of the closure relatively small

    val edges = partlyRenamedEdgesGroupedWithNewIdsOfDestinations
      .flatMap({ case (srcList, idList) => srcList.flatMap(src => idList.map(dstId => (src, dstId))) })

    new GraphIDAssignerOutput(newVertices, edges, newIdsToOld)
  }

  /**
   * Return value for the ID assigner.
   * @param vertices vertex list of renamed graph
   * @param edges edge list of renamed graph
   * @param newIdsToOld  pairs mapping new IDs to their corresponding vertices in the base graph
   * @tparam T Type of the vertex IDs in the input graph
   */
  case class GraphIDAssignerOutput[T: ClassTag](vertices: RDD[Long],
                                                edges: RDD[(Long, Long)],
                                                newIdsToOld: RDD[(Long, T)])

}
