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

package org.trustedanalytics.atk.graphbuilder.write.dao

import java.util

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property }
import com.tinkerpop.blueprints
import com.tinkerpop.blueprints.{ Direction, Graph }

import scala.util.Try

/**
 * Data access for Edges using Blueprints API
 *
 * @param graph the Blueprints Graph
 * @param vertexDAO needed to look up the end points
 */
class EdgeDAO(graph: Graph, vertexDAO: VertexDAO) extends Serializable {

  if (graph == null) {
    throw new IllegalArgumentException("EdgeDAO requires a non-null Graph")
  }

  /**
   * Find the Blueprints Edge using the Vertex ID's and label of the supplied Edge
   * @param edge the GraphBuilder Edge find
   * @return the Blueprints Edge
   */
  def find(edge: GBEdge): Option[blueprints.Edge] = {
    find(edge.tailVertexGbId, edge.headVertexGbId, edge.label)
  }

  /**
   * Find the Blueprints Edge using the supplied Vertex ID's and label
   * @param tailGbId the source of the Edge
   * @param headGbId the destination of the Edge
   * @param label the Edge label
   * @return the Blueprints Edge
   */
  def find(tailGbId: Property, headGbId: Property, label: String): Option[blueprints.Edge] = {
    val tailVertex = vertexDAO.findByGbId(tailGbId)
    val headVertex = vertexDAO.findByGbId(headGbId)

    if (tailVertex.isEmpty || headVertex.isEmpty) {
      None
    }
    else {
      find(tailVertex.get, headVertex.get, label)
    }
  }

  /**
   * Find the Blueprints Edge using the supplied parameters.
   * @param tailVertex the source of the Edge
   * @param headVertex the destination of the Edge
   * @param label the Edge label
   * @return the Blueprints Edge
   */
  def find(tailVertex: blueprints.Vertex, headVertex: blueprints.Vertex, label: String): Option[blueprints.Edge] = {
    val edgeIterator = Try(
      tailVertex.query().direction(Direction.OUT).labels(label).edges.iterator()
    ).getOrElse(
        //Return empty iterator if label not defined in schema
        new util.ArrayList[com.tinkerpop.blueprints.Edge]().iterator()
      )

    while (edgeIterator.hasNext) {
      val blueprintsEdge = edgeIterator.next()
      if (blueprintsEdge.getVertex(Direction.IN) == headVertex) {
        return Some(blueprintsEdge)
      }
    }
    None
  }

  /**
   * Create a new blueprints.Edge from the supplied Edge and set all properties
   * @param edge the description of the Edge to create
   * @return the newly created Edge
   */
  def create(edge: GBEdge): blueprints.Edge = {
    val tailVertex = vertexDAO.findById(edge.tailPhysicalId, edge.tailVertexGbId)
    val headVertex = vertexDAO.findById(edge.headPhysicalId, edge.headVertexGbId)
    if (tailVertex.isEmpty || headVertex.isEmpty) {
      throw new IllegalArgumentException("Vertex was missing, can't insert edge: " + edge)
    }
    val blueprintsEdge = graph.addEdge(null, tailVertex.get, headVertex.get, edge.label)
    update(edge, blueprintsEdge)
  }

  /**
   * Copy all properties from the supplied GB Edge to the Blueprints Edge
   * @param edge from
   * @param blueprintsEdge to
   * @return the blueprints.Edge
   */
  def update(edge: GBEdge, blueprintsEdge: blueprints.Edge): blueprints.Edge = {
    edge.properties.foreach(property => {
      if (property.value != null) {
        blueprintsEdge.setProperty(property.key, property.value)
      }
      else {
        // null values aren't supported by bluesprints, so you remove the property
        blueprintsEdge.removeProperty(property.key)
      }

    })
    blueprintsEdge
  }

  /**
   * If it exists, find and update the existing edge, otherwise create a new one
   * @param edge the description of the Edge to create
   * @return the newly created Edge
   */
  def updateOrCreate(edge: GBEdge): blueprints.Edge = {
    val blueprintsEdge = find(edge)
    if (blueprintsEdge.isEmpty) {
      create(edge)
    }
    else {
      update(edge, blueprintsEdge.get)
    }
  }

}
