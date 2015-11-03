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

import org.apache.commons.lang3.StringUtils

/**
 * A property definition is either of type Edge or Vertex
 */
object PropertyType extends Enumeration {
  val Vertex, Edge = Value
}

/**
 * Schema definition for a Property
 *
 * @param propertyType this property is either for a Vertex or an Edge
 * @param name property name
 * @param dataType data type
 * @param unique True if this property is unique
 * @param indexed True if this property should be indexed
 */
case class PropertyDef(propertyType: PropertyType.Value, name: String, dataType: Class[_], unique: Boolean = false, indexed: Boolean = false) {

  if (StringUtils.isEmpty(name)) {
    throw new IllegalArgumentException("property name can't be empty")
  }

  // TODO: in the future, possibly add support for indexName

}
