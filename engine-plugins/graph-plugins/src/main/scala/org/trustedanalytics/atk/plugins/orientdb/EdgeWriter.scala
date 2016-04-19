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
import com.tinkerpop.blueprints.impls.orient.{ OrientEdge, OrientGraph }
import org.apache.spark.atk.graph.Edge
import org.trustedanalytics.atk.domain.schema.GraphSchema
import org.trustedanalytics.atk.engine.frame.RowWrapper

/**
 * Created by wtaie on 4/18/16.
 */
class EdgeWriter {

  /**
   * Method for exporting an edge
   * @param oGraph     an instance of Orient graph database
   * @param edge       he atk edge that is required to be exported to OrientEdge
   * @param srcVertex  is a blueprintsVertex as a source
   * @param destVertex is a blueprintsVertex as a destination
   * @return OrientEdge
   */
  def addEdge(oGraph: OrientGraph, edge: Edge, srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): OrientEdge = {

    require(oGraph != null, "The Orient graph database instance must not equal null")
    val className = "Edge" + edge.schema.label
    if (oGraph.getEdgeType(className) == null) {
      val createEdgeSchema = new EdgeSchemaWriter
      val oEdgeType = createEdgeSchema.createEdgeSchema(oGraph, edge.schema)
    }
    val oEdge = oGraph.addEdge("class:" + className, srcVertex, destVertex, className)
    val rowWrapper = new RowWrapper(edge.schema)
    edge.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        oEdge.setProperty(col.name, rowWrapper(edge.row).value(col.name))
      }
    })
    oEdge
  }

}
