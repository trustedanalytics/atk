/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.plugins.labelpropagation

import org.apache.spark.graphx.lib.{ ConnectedComponents => GraphXConnectedComponents, LabelPropagation }
import org.apache.spark.graphx.{ Edge => GraphXEdge, _ }
import org.apache.spark.rdd.RDD

/**
 *
 */

object LabelPropagationDefault {

  def run(vertexList: RDD[Long], edgeList: RDD[(Long, Long)], maxSteps: Int): RDD[(Long, Long)] = {

    val graphXVertices: RDD[(Long, Null)] = vertexList.map((vid: Long) => (vid, null))
    val graphXEdges: RDD[GraphXEdge[Null]] = edgeList.map(edge => new GraphXEdge[Null](edge._1, edge._2, null))

    val graph: Graph[Null, Null] = Graph(graphXVertices, graphXEdges).partitionBy(PartitionStrategy.RandomVertexCut)
    val labeledGraph = LabelPropagation.run(graph, maxSteps)

    val out: RDD[(Long, Long)] = labeledGraph.vertices.map({
      case (vertexId, labelAttribute) => (vertexId, labelAttribute.toLong)
    })

    out
  }

}
