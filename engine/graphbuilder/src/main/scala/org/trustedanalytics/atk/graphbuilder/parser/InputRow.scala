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


package org.trustedanalytics.atk.graphbuilder.parser

import org.apache.commons.lang3.StringUtils

/**
 * Wrapper for rows to simplify parsing.
 * <p>
 * An input row is row of raw data plus a schema
 * </p>
 */
class InputRow(inputSchema: InputSchema, row: Seq[Any]) {

  if (row.size != inputSchema.size) {
    throw new IllegalArgumentException(s"Input row (${row.size}) should have the same number of columns as the inputSchema (${inputSchema.size})")
  }

  /**
   * The data or value from a column.
   */
  def value(columnName: String): Any = {
    row(inputSchema.columnIndex(columnName))
  }

  /**
   * Column is considered empty if it is null, None, Nil, or the empty String
   */
  def columnIsNotEmpty(columnName: String): Boolean = {
    val any = row(inputSchema.columnIndex(columnName))
    any match {
      case null => false
      case None => false
      case Nil => false
      case s: String => StringUtils.isNotEmpty(s)
      case _ => true
    }
  }

}
