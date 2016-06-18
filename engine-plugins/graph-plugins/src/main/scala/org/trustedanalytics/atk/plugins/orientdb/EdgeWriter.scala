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

import com.tinkerpop.blueprints.{ Vertex => BlueprintsVertex, Edge => BlueprintsEdge }
import com.tinkerpop.blueprints.impls.orient.{ OrientGraphNoTx, OrientEdge }
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

  require(orientGraph != null, "The OrientDB graph database instance must not equal null")

  /**
   * Method for exporting an edge
   *
   * @param srcVertex  is a blueprintsVertex as a source
   * @param destVertex is a blueprintsVertex as a destination
   * @return OrientDB edge
   */
  def create(srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): OrientEdge = {

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

  /**
   * a method that finds OrientDB edge
   * @param edge ATK edge
   * @return OrientDB edge
   */
  def find(edge: Edge): Option[BlueprintsEdge] = {
    val edgeIterator = orientGraph.getEdges(GraphSchema.srcVidProperty == edge.srcVertexId() && GraphSchema.destVidProperty == edge.destVertexId()).iterator()
    if (edgeIterator.hasNext) {
      val existingEdge = edgeIterator.next()
      return Some(existingEdge)
    }
    None
  }

  /**
   * a method that updates OrientDB edge
   * @param edge ATK edge
   * @param orientDbEdge OrientDB edge
   * @return updated OrientDB edge
   */
  def update(edge: Edge, orientDbEdge: BlueprintsEdge): BlueprintsEdge = {
    val rowWrapper = new RowWrapper(edge.schema)
    edge.schema.columns.foreach(col => {
      if (col.name != GraphSchema.labelProperty) {
        orientDbEdge.setProperty(col.name, rowWrapper(edge.row).value(col.name))
      }
    })
    orientDbEdge
  }

  /**
   * a method that updates OrientDB edge if exists or creates a new edge if not found
   * @param edge ATK edge
   * @param srcVertex OrientDB vertex as a source
   * @param destVertex OrientDB vertex as a destination
   * @return OrientDB edge
   */
  def updateOrCreate(edge: Edge, srcVertex: BlueprintsVertex, destVertex: BlueprintsVertex): BlueprintsEdge = {
    val orientEdge = find(edge)
    val newEdge = if (orientEdge.isEmpty) {
      create(srcVertex, destVertex)
    }
    else {
      update(edge, orientEdge.get)
    }
    newEdge
  }

}
