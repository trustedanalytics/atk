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

import org.trustedanalytics.atk.graphbuilder.parser.InputSchema

/**
 * Figure out the dataType of the supplied value using the InputSchema, if needed.
 */
class DataTypeResolver(inputSchema: InputSchema) extends Serializable {

  /**
   * Figure out the dataType of the supplied value using the InputSchema, if needed.
   */
  def get(value: Value): Class[_] = {
    value match {
      case constant: ConstantValue => constant.value.getClass
      case parsed: ParsedValue =>
        val dataType = inputSchema.columnType(parsed.columnName)
        if (dataType == null) {
          throw new RuntimeException("InputSchema did NOT define a dataType for column: " + parsed.columnName
            + ". Please supply the type or don't infer the schema from the rules.")
        }
        dataType
      case compound: CompoundValue => classOf[String]
      case _ => throw new RuntimeException("Unexpected type of value is not yet implemented: " + value)
    }
  }
}
