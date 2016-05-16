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

import com.tinkerpop.blueprints.impls.orient.{OrientEdge, OrientGraphNoTx}
import com.tinkerpop.blueprints.{ Edge => BlueprintsEdge }
import org.apache.spark.atk.graph.Edge
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{GraphSchema, EdgeSchema}

class EdgeReader (graph: OrientGraphNoTx, edgeSchema: EdgeSchema, srcVertexId:Long){

  /**
    *
    * @return
    */
  def importEdge(): Edge = {
    try{
      val orientEdge = getOrientEdge
      createEdge(orientEdge)
    }catch{
      case e: Exception =>
        throw new RuntimeException(s"Unable to read edge with source ID $srcVertexId from OrientDB graph: ${e.getMessage}")
    }
  }

  /**
    *
    * @param orientEdge
    * @return
    */
  def createEdge(orientEdge: BlueprintsEdge): Edge = {

    val row = edgeSchema.columns.map(col => {
      if (col.name == GraphSchema.labelProperty) {
       edgeSchema.label

      }else{
        val prop: Any = orientEdge.getProperty(col.name)
        prop
      }
    })
    new Edge(edgeSchema,Row.fromSeq(row.toSeq))
  }

  /**
    *
    * @return
    */
  def getOrientEdge: BlueprintsEdge  = {
    val edgeIterator = graph.getEdges(GraphSchema.srcVidProperty, srcVertexId).iterator()
      edgeIterator.next()

  }
}