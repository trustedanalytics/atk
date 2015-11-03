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


package org.trustedanalytics.atk.graphbuilder.schema

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }

import scala.collection.mutable.Map

/**
 * Infer the schema from the GraphElements themselves after parsing.
 *
 * This is more overhead because it requires passing over all of the data but allows
 * for cases like dynamic labels, i.e. labels that are parsed from the input.
 */
class InferSchemaFromData extends Serializable {

  var edgeLabelDefsMap = Map[String, EdgeLabelDef]()
  var propertyDefsMap = Map[String, PropertyDef]()

  /**
   * Add an Edge to the inferred schema.
   */
  def add(edge: GBEdge): Unit = {
    addEdgeLabel(edge)
    addProperties(PropertyType.Edge, edge.properties)
  }

  /**
   * Add a label to the map, if it isn't already there.
   */
  def addEdgeLabel(edge: GBEdge): Unit = {
    if (edgeLabelDefsMap.get(edge.label).isEmpty) {
      edgeLabelDefsMap += (edge.label -> new EdgeLabelDef(edge.label))
    }
  }

  /**
   * Add a Vertex to the inferred schema.
   */
  def add(vertex: GBVertex): Unit = {
    addProperty(PropertyType.Vertex, vertex.gbId, isGbId = true)
    addProperties(PropertyType.Vertex, vertex.properties)
  }

  /**
   * Get the inferred GraphSchema.  Do this after adding Edges and Vertices.
   */
  def graphSchema: GraphSchema = {
    new GraphSchema(edgeLabelDefsMap.values.toList, propertyDefsMap.values.toList)
  }

  /**
   * Add a list of properties, if they aren't already present.
   */
  private def addProperties(propertyType: PropertyType.Value, properties: Set[Property]): Unit = {
    properties.foreach(prop => addProperty(propertyType, prop, isGbId = false))

  }

  /**
   * Add a property, if it isn't already present.
   * @param propertyType Vertex or Edge
   * @param property property to add
   * @param isGbId true if this property is a Vertex gbId
   */
  private def addProperty(propertyType: PropertyType.Value, property: Property, isGbId: Boolean): Unit = {
    if (propertyDefsMap.get(property.key).isEmpty && property.value != null) {
      propertyDefsMap += (property.key -> new PropertyDef(propertyType, property.key, property.value.getClass, isGbId, isGbId))
    }
  }

}
