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

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx
import org.apache.spark.atk.graph.Vertex
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ VertexSchema,GraphSchema}
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }

class VertexReader(graph: OrientGraphNoTx, vertexSchema: VertexSchema, vertexId: Long) {

  /**
    *
    * @return
    */
  def importVertex():Vertex ={
    try{
    val orientVertex = getOrientVertex
    createVertex(orientVertex)

  }catch{
      case e: Exception =>
        throw new RuntimeException(s"Unable to read vertex with ID $vertexId from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
    * @param orientVertex
    * @return
    */
  def createVertex(orientVertex: BlueprintsVertex): Vertex = {

    val row = vertexSchema.columns.map(col => {
      if (col.name == GraphSchema.labelProperty) {
        vertexSchema.label
      }else{
        val prop: Any = orientVertex.getProperty(col.name)
        prop
      }
    })
    new Vertex(vertexSchema,Row.fromSeq(row.toSeq))
  }

  /**
    *
    * @return
    */
  def getOrientVertex: BlueprintsVertex  = {
    val vertexIterator = graph.getVertices(GraphSchema.vidProperty, vertexId).iterator()
    val orientVertex = vertexIterator.next()
    orientVertex
  }
}
