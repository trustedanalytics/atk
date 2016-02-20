/**
 *  Copyright (c) 2016 Intel Corporation 
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
 * Marker interface for GraphElements (Vertices and Edges).
 * <p>
 * This can be used if you want to parse both Edges and Vertices in one pass over the input into a single stream.
 * </p>
 */
trait GraphElement {

  /**
   * Find a property in the property list by key
   * @param key Property key
   * @return Matching property
   */
  def getProperty(key: String): Option[Property]

  /**
   * Get property values to a array in the order specified.
   * @param columns specifed columns to retrieve property values
   * @param properties properties
   * @return array of column values
   */
  def getPropertiesValueByColumns(columns: List[String], properties: Set[Property]): Array[Any] = {
    val mapping = properties.map(p => p.key -> p.value).toMap
    columns.map(c => mapping(c)).toArray
  }
}
