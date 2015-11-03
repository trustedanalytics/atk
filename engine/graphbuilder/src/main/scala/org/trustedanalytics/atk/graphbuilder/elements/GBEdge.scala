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


package org.trustedanalytics.atk.graphbuilder.elements

import org.trustedanalytics.atk.graphbuilder.util.StringUtils

/**
 * An Edge links two Vertices.
 * <p>
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 * </p>
 * @param tailPhysicalId the unique Physical ID for the source Vertex from the underlying Graph storage layer (optional)
 * @param headPhysicalId the unique Physical ID for the destination Vertex from the underlying Graph storage layer (optional)
 * @param tailVertexGbId the unique ID for the source Vertex
 * @param headVertexGbId the unique ID for the destination Vertex
 * @param label the Edge label
 * @param properties the set of properties associated with this edge
 */
case class GBEdge(eid: Option[Long], var tailPhysicalId: Any, var headPhysicalId: Any, tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Set[Property]) extends GraphElement with Mergeable[GBEdge] {

  def this(eid: Option[Long], tailVertexGbId: Property, headVertexGbId: Property, label: String, properties: Set[Property]) {
    this(eid, null, null, tailVertexGbId, headVertexGbId, label, properties)
  }

  def this(eid: Option[Long], tailVertexGbId: Property, headVertexGbId: Property, label: Any, properties: Set[Property]) {
    this(eid, tailVertexGbId, headVertexGbId, StringUtils.nullSafeToString(label), properties)
  }

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  override def id: Any = {
    (tailVertexGbId, headVertexGbId, label)
  }

  /**
   * Merge properties for two edges.
   *
   * Conflicts are handled arbitrarily.
   *
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: GBEdge): GBEdge = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge edges with different ids or labels")
    }
    new GBEdge(eid, tailVertexGbId, headVertexGbId, label, Property.merge(this.properties, other.properties))
  }

  /**
   * Create edge with head and tail in reverse order
   */
  def reverse(): GBEdge = {
    new GBEdge(eid, headVertexGbId, tailVertexGbId, label, properties)
  }

  /**
   * Find a property in the property list by key
   * @param key Property key
   * @return Matching property
   */
  override def getProperty(key: String): Option[Property] = {
    properties.find(p => p.key == key)
  }

  /**
   * Get a property value as String if this key exists
   * @param key Property key
   * @return Matching property value, or empty string if no such property
   */
  override def getPropertyValueAsString(key: String): String = {
    val result = for {
      property <- this.getProperty(key)
    } yield property.value.toString
    result.getOrElse("")
  }
}
