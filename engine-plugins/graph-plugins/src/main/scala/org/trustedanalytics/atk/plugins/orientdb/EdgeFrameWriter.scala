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
import org.trustedanalytics.atk.domain.schema.GraphSchema

/**
 *  Exports EdgeFrameRdd to OrientDB edges
 *
 * @param edgeFrameRdd edges frame to be exported to Orient
 */

class EdgeFrameWriter(edgeFrameRdd: EdgeFrameRdd, dbConfigurations: DbConfigurations) extends Serializable {

  /**
   * Method to export edge frame to OrientDb
   *
   * @param dbName OrientDb URI
   * @param batchSize the number of edges to be commited
   * @return the number of exported edges
   */
  def exportEdgeFrame(dbName: String, batchSize: Int): Long = {

    val edgesCountRdd = edgeFrameRdd.mapPartitionEdges(iter => {
      val orientGraph = GraphDbFactory.graphDbConnector(dbName, dbConfigurations)
      var batchCounter = 0L
      try {
        while (iter.hasNext) {
          val edgeWrapper = iter.next()
          val edge = edgeWrapper.toEdge
          // lookup the source and destination vertices
          val findOrientVertex = new VertexWriter(orientGraph)
          val srcVertex = findOrientVertex.findOrCreateVertex(edge.srcVertexId())
          val destVertex = findOrientVertex.findOrCreateVertex(edge.destVertexId())
          val edgeWriter = new EdgeWriter(orientGraph, edge)
          val orientEdge = edgeWriter.addEdge(srcVertex, destVertex)
          batchCounter += 1
          if (batchCounter % batchSize == 0 && batchCounter != 0) {
            orientGraph.commit()
          }
        }
      }
      catch {
        case e: Exception => {
          orientGraph.rollback()
          throw new RuntimeException("Unable to add edges to OrientDB graph", e)
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
