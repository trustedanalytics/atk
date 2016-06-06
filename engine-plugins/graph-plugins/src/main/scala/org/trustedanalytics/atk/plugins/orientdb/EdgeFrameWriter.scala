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
import org.trustedanalytics.atk.event.EventLogging

/**
 * Exports EdgeFrameRdd to OrientDB edges
 *
 * @param edgeFrameRdd the edges frame to be exported to OrientDB
 * @param dbConfigurations OrientDB configurations
 */
class EdgeFrameWriter(edgeFrameRdd: EdgeFrameRdd, dbConfigurations: DbConfiguration) extends Serializable with EventLogging {

  /**
   * Method to export edge frame to OrientDB
   *
   * @param batchSize the number of edges to be committed
   * @return the number of exported edges
   */
  def exportEdgeFrame(batchSize: Int): Long = {

    val edgesCountRdd = edgeFrameRdd.mapPartitionEdges(iter => {
      val orientGraph = GraphDbFactory.graphDbConnector(dbConfigurations)
      var batchCounter = 0L
      try {
        while (iter.hasNext) {
          val edgeWrapper = iter.next()
          val edge = edgeWrapper.toEdge
          // lookup the source and destination vertices
          val findOrientVertex = new VertexWriter(orientGraph)
          val srcVertex = findOrientVertex.findOrCreate(edge.srcVertexId())
          val destVertex = findOrientVertex.findOrCreate(edge.destVertexId())
          val edgeWriter = new EdgeWriter(orientGraph, edge)
          val orientEdge = if (dbConfigurations.append) {
            edgeWriter.updateOrCreate(edge, srcVertex, destVertex)
          }
          else {
            edgeWriter.create(srcVertex, destVertex)
          }
          batchCounter += 1
          if (batchCounter % batchSize == 0 && batchCounter != 0) {
            orientGraph.commit()
          }
        }
      }
      catch {
        case e: Exception => {
          orientGraph.rollback()
          error(s"Unable to add edges to OrientDB graph", exception = e)
          throw new RuntimeException(s"Unable to add edges to OrientDB graph: ${e.getMessage}")
        }
      }
      finally {
        orientGraph.shutdown(true, true) //commit the changes and close the graph
      }
      Array(batchCounter).toIterator
    })
    edgesCountRdd.sum().toLong
  }

}
