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

import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex }
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientEdge, OrientGraph }
import org.apache.spark.atk.graph.Edge
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Exports edge to OrientDB edge
 *
 * @param orientGraph an instance of OrientDB graph database
 * @param edge atk edge to be exported to OrientDB edge
 */
class EdgeWriter(orientGraph: OrientGraphNoTx, edge: Edge) {

  require(orientGraph != null, "The Orient graph database instance must not equal null")

  /**
   * Method for exporting an edge
   *
   * @param srcVertex  is a blueprintsVertex as a source
   * @param destVertex is a blueprintsVertex as a destination
   * @return OrientDB edge
   */
  def addEdge(srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): OrientEdge = {

    val className = edge.schema.label
    val orientEdge = orientGraph.addEdge("class:" + className, srcVertex, destVertex, className)
    val rowWrapper = new RowWrapper(edge.schema)
    edge.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        orientEdge.setProperty(col.name, rowWrapper(edge.row).value(col.name))
      }
    })
    orientEdge
  }

}
