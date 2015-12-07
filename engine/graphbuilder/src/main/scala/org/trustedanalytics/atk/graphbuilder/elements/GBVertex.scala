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

/**
 * A Vertex.
 * <p>
 * GB Id's are special properties that uniquely identify Vertices.  They are special, in that they must uniquely
 * identify vertices, but otherwise they are completely normal properties.
 * </p>
 * @constructor create a new Vertex
 * @param physicalId the unique Physical ID for the Vertex from the underlying Graph storage layer (optional)
 * @param gbId the unique id that will be used by graph builder
 * @param properties the other properties that exist on this vertex
 *
 */
case class GBVertex(physicalId: Any, gbId: Property, properties: Set[Property]) extends GraphElement with Mergeable[GBVertex] {

  def this(gbId: Property, properties: Set[Property]) {
    this(null, gbId, properties)
  }

  if (gbId == null) {
    throw new IllegalArgumentException("gbId can't be null")
  }

  /**
   * Merge-ables with the same id can be merged together.
   *
   * (In Spark, you would use this as the unique id in the groupBy before merging duplicates)
   */
  override def id: Any = gbId

  /**
   * Merge two Vertices with the same id into a single Vertex with a combined list of properties.
   *
   * Conflicts are handled arbitrarily.
   *
   * @param other item to merge
   * @return the new merged item
   */
  override def merge(other: GBVertex): GBVertex = {
    if (id != other.id) {
      throw new IllegalArgumentException("You can't merge vertices with different ids")
    }

    if (physicalId != null && other.physicalId == null) {
      new GBVertex(physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else if (physicalId == null && other.physicalId != null) {
      new GBVertex(other.physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else if (physicalId != null && physicalId == other.physicalId) {
      new GBVertex(physicalId, gbId, Property.merge(this.properties, other.properties))
    }
    else {
      new GBVertex(gbId, Property.merge(this.properties, other.properties))
    }
  }

  /**
   * Full set of properties including the gbId
   */
  def fullProperties: Set[Property] = {
    properties + gbId
  }

  /**
   * Find a property in the property list by key
   *
   * @param key Property key
   * @return Matching property
   */
  override def getProperty(key: String): Option[Property] = {
    properties.find(p => p.key == key)
  }
}
