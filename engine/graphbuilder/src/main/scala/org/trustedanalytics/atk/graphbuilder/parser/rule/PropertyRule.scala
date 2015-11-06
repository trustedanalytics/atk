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

package org.trustedanalytics.atk.graphbuilder.parser.rule

import org.trustedanalytics.atk.graphbuilder.parser.InputRow

/**
 * A rule definition for how to Parse properties from columns
 *
 * It is helpful to import ValueDSL._ when creating rules
 *
 * @param key the name for the property
 * @param value the value for the property
 */
class PropertyRule(val key: Value, val value: Value) extends Serializable {

  /**
   * Create a simple property where the source columnName is also the destination property name
   * @param columnName from input row
   */
  def this(columnName: String) {
    this(new ConstantValue(columnName), new ParsedValue(columnName))
  }

  /**
   * Does this rule apply to the supplied row
   */
  def appliesTo(row: InputRow): Boolean = {
    value.in(row)
  }
}
