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

/**
 * Represents the definition of a Graph Schema
 *
 * @param edgeLabelDefs edge label definitions (label names)
 * @param propertyDefs property definitions (names, indexes, uniqueness)
 */
class GraphSchema(val edgeLabelDefs: List[EdgeLabelDef], val propertyDefs: List[PropertyDef]) {

  /**
   * Get a list of zero or more properties with the supplied name
   */
  def propertiesWithName(propertyName: String): List[PropertyDef] = {
    propertyDefs.filter(propertyDef => propertyDef.name == propertyName)
  }

  /**
   * The number of items defined in this GraphSchema
   */
  val count: Int = edgeLabelDefs.size + propertyDefs.size

}
