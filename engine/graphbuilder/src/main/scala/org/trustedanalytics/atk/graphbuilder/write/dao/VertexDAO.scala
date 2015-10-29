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

import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }
import com.tinkerpop.blueprints
import com.tinkerpop.blueprints.Graph

/**
 * Data access for Vertices using Blueprints API
 */
class VertexDAO(graph: Graph) extends Serializable {

  if (graph == null) {
    throw new IllegalArgumentException("VertexDAO requires a non-null Graph")
  }

  /**
   * Convenience method for looking up a Vertex by either of two ids
   * @param physicalId if not null, perform lookup with this id
   * @param gbId otherwise use this id
   * @return the blueprints Vertex
   */
  def findById(physicalId: Any, gbId: Property): Option[blueprints.Vertex] = {
    if (physicalId != null) findByPhysicalId(physicalId)
    else findByGbId(gbId)
  }

  /**
   * Find a Vertex by the physicalId of the underlying Graph storage layer
   * @param id the physicalId
   */
  def findByPhysicalId(id: Any): Option[blueprints.Vertex] = {
    Option(graph.getVertex(id))
  }

  /**
   * Find a blueprints Vertex by the supplied gbId
   */
  def findByGbId(gbId: Property): Option[blueprints.Vertex] = {
    if (gbId == null) {
      None
    }
    else {
      val vertices = graph.getVertices(gbId.key, gbId.value)
      val i = vertices.iterator()
      if (i.hasNext) {
        Some(i.next())
      }
      else {
        None
      }
    }
  }

  /**
   * Create a new blueprints.Vertex from the supplied Vertex and set all properties
   * @param vertex the description of the Vertex to create
   * @return the newly created Vertex
   */
  def create(vertex: GBVertex): blueprints.Vertex = {
    val blueprintsVertex = graph.addVertex(null)
    update(vertex, blueprintsVertex)
  }

  /**
   * Copy all properties from the supplied GB vertex to the Blueprints Vertex.
   * @param vertex from
   * @param blueprintsVertex to
   * @return the blueprints.Vertex
   */
  def update(vertex: GBVertex, blueprintsVertex: blueprints.Vertex): blueprints.Vertex = {
    vertex.fullProperties.foreach(property => {
      if (property.value != null) {
        blueprintsVertex.setProperty(property.key, property.value)
      }
      else {
        // null values aren't supprted by bluesprints, so you remove the property
        blueprintsVertex.removeProperty(property.key)
      }

    })
    blueprintsVertex
  }

  /**
   * If it exists, find and update the existing vertex, otherwise create a new one
   * @param vertex the description of the Vertex to create
   * @return the newly created Vertex
   */
  def updateOrCreate(vertex: GBVertex): blueprints.Vertex = {
    // val blueprintsVertex = findByGbId(vertex.gbId)
    val blueprintsVertex = findById(vertex.physicalId, vertex.gbId)
    if (blueprintsVertex.isEmpty) {
      create(vertex)
    }
    else {
      update(vertex, blueprintsVertex.get)
    }
  }

}
