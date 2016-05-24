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
import com.tinkerpop.blueprints.{ Edge => BlueprintsEdge }
import org.apache.spark.atk.graph.Edge
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, EdgeSchema }
import org.trustedanalytics.atk.event.EventLogging

/**
 * imports OrientDB edge to ATK edge
 *
 * @param graph OrientDB graph database
 * @param edgeSchema ATK edge schema
 */
class EdgeReader(graph: OrientGraphNoTx, edgeSchema: EdgeSchema) extends EventLogging {

  /**
   * A method imports OrientDB edge from OrientDB database to ATK edge
   *
   * @return ATK edge
   */
  def importEdge(orientEdge: BlueprintsEdge): Edge = {
    try {
      createEdge(orientEdge)
    }
    catch {
      case e: Exception =>
        error(s"Unable to read edge of ID ${orientEdge.getId.toString} from OrientDB graph", exception = e)
        throw new RuntimeException(s"Unable to read edge of ID ${orientEdge.getId.toString} from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
   * A method creates ATK edge
   *
   * @param orientEdge OrientDB edge
   * @return ATK edge
   */
  def createEdge(orientEdge: BlueprintsEdge): Edge = {

    val row = edgeSchema.columns.map(col => {
      if (col.name == GraphSchema.labelProperty) {
        edgeSchema.label
      }
      else {
        val prop: Any = orientEdge.getProperty(col.name)
        prop
      }
    })
    new Edge(edgeSchema, Row.fromSeq(row.toSeq))
  }
}