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
import org.trustedanalytics.atk.domain.schema.{ VertexSchema, GraphSchema }
import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import org.trustedanalytics.atk.event.EventLogging

/**
 * imports OrientDB vertex from OrientDB database to ATK vertex
 *
 * @param graph OrientDB database
 * @param vertexSchema ATK vertex Schema
 */
class VertexReader(graph: OrientGraphNoTx, vertexSchema: VertexSchema) extends EventLogging {

  /**
   * A method imports OrientDB vertex to ATK vertex
   *
   * @param orientVertex OrientDB vertex
   * @return ATK vertex
   */
  def importVertex(orientVertex: BlueprintsVertex): Vertex = {
    try {
      createVertex(orientVertex)
    }
    catch {
      case e: Exception =>
        error(s"Unable to read vertex with ID ${orientVertex.getId.toString} from OrientDB graph", exception = e)
        throw new RuntimeException(s"Unable to read vertex with ID ${orientVertex.getId.toString} from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK vertex
   *
   * @param orientVertex OrientDB vertex
   * @return ATK vertex
   */
  def createVertex(orientVertex: BlueprintsVertex): Vertex = {

    val row = vertexSchema.columns.map(col => {
      if (col.name == GraphSchema.labelProperty) {
        vertexSchema.label
      }
      else {
        orientVertex.getProperty(col.name): Any
      }
    })
    new Vertex(vertexSchema, Row.fromSeq(row.toSeq))
  }
}
