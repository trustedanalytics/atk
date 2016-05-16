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
package org.trustedanalytics.atk.plugins.orientdbimport

import com.tinkerpop.blueprints.Direction
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.GraphSchema
import scala.collection.mutable.ArrayBuffer

class LoadEdgeFrame(graph: OrientGraphNoTx) {
  def importOrientDbEdgeClass(): List[Row] = {

    val schemaReader = new SchemaReader(graph)
    val edgeSchema = schemaReader.importEdgeSchema()
    val edgeBuffer = new ArrayBuffer[Row]()
    val edgeCount = graph.countEdges(edgeSchema.label)
    var count = 1
    while(edgeCount !=0 && count <= edgeCount) {
      val srcVertexId: Long = graph.getEdgesOfClass(edgeSchema.label).iterator().next().getVertex(Direction.OUT).getProperty(GraphSchema.vidProperty)
      val edgeReader = new EdgeReader(graph, edgeSchema, srcVertexId)
      val edge = edgeReader.importEdge()
      edgeBuffer += edge.row
      count += 1
    }
    edgeBuffer.toList
  }

}
