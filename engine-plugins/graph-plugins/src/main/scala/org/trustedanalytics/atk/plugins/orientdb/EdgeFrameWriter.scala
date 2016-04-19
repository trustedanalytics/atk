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
package org.trustedanalytics.atk.plugins.orientdb

import org.apache.spark.atk.graph.EdgeFrameRdd

/**
 * Created by wtaie on 4/18/16.
 */
class EdgeFrameWriter {
  /**
   * Method to export edge frame to OrientDb
   *
   * @param dbUri OrientDb URI
   * @param edgeFrameRdd edges frame to be exported to Orient
   * @param batchSize the number of edges to be commited
   * @return the number of exported edges
   */
  def exportEdgeFrame(dbUri: String, edgeFrameRdd: EdgeFrameRdd, batchSize: Int): Long = {

    val edgesCountRdd = edgeFrameRdd.mapPartitionEdges(iter => {
      val graphFactory = new GraphDbFactory
      val oGraph = graphFactory.GraphDbConnector(dbUri)
      var batchCounter = 0L
      while (iter.hasNext) {
        val edgeWrapper = iter.next()
        val edge = edgeWrapper.toEdge
        // lookup the source and destination vertices
        val srcVertex = oGraph.getVertices("_vid", edge.srcVertexId()).iterator().next()
        val destVertex = oGraph.getVertices("_vid", edge.destVertexId()).iterator().next()
        val oEdgeWriter = new EdgeWriter
        val oEdge = oEdgeWriter.addEdge(oGraph, edge, srcVertex, destVertex)
        batchCounter += 1
        if (batchCounter % batchSize == 0 && batchCounter != 0) {
          oGraph.commit()
        }
      }
      oGraph.shutdown(true, true) //commit the changes and close the graph
      Array(batchCounter).toIterator
    })
    edgesCountRdd.sum().toLong
  }

}
