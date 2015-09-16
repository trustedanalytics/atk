/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.graphbuilder.parser

import org.trustedanalytics.atk.graphbuilder.util.PrimitiveConverter
import org.apache.commons.lang3.StringUtils

/**
 * Column Definition including the Index of the Column
 *
 * @param columnName the name of the column
 * @param dataType the dataType of the column (needed to infer the schema from the parsing rules)
 * @param columnIndex the index position of the column (if null, index will be inferred from the column order)
 */
case class ColumnDef(columnName: String, dataType: Class[_], columnIndex: Integer) {

  if (StringUtils.isEmpty(columnName)) {
    throw new IllegalArgumentException("columnName cannot be empty")
  }
  if (columnIndex != null && columnIndex < 0) {
    throw new IllegalArgumentException("columnIndex needs to be greater than or equal to zero")
  }

  /**
   * Index will be assumed based on the order of the columns
   * @param columnName the name of the column
   * @param dataType the dataType of the column (needed to infer the schema from the parsing rules)
   */
  def this(columnName: String, dataType: Class[_]) {
    this(columnName, dataType, null)
  }
}

/**
 * Defines the schema of the rows of input
 * @param columns the definitions for each column
 */
case class InputSchema(columns: Seq[ColumnDef]) extends Serializable {

  /**
   * Convenience constructor for creating InputSchemas when all of the columns are the same type
   * @param columnNames the column names as the appear in order
   * @param columnType the dataType shared by all columns (needed to infer the schema from the parsing rules)
   */
  def this(columnNames: Seq[String], columnType: Class[_]) {
    this(columnNames.map(columnName => new ColumnDef(columnName, columnType)))
  }

  @transient
  private lazy val schema = {
    var schema = Map[String, ColumnDef]()
    var columnIndex = 0
    for (column <- columns) {
      schema = schema + (column.columnName -> {
        if (column.columnIndex == null) column.copy(columnIndex = columnIndex)
        else column
      })
      columnIndex += 1
    }
    schema
  }

  /**
   * Lookup the index of a column by name
   */
  def columnIndex(columnName: String): Int = {
    schema(columnName).columnIndex
  }

  /**
   * Lookup the type of a column by name
   */
  def columnType(columnName: String): Class[_] = {
    schema(columnName).dataType
  }

  /**
   * Number of columns in the schema
   */
  def size: Int = {
    schema.size
  }

  /**
   * Convert dataTypes of Class[primitive] to their Object equivalents
   *
   * e.g. classOf[scala.Int] to classOf[java.lang.Integer]
   *      classOf[scala.Long] to classOf[java.lang.Long]
   *      classOf[scala.Char] to classOf[java.lang.Char]
   *      etc.
   *
   * Titan doesn't support primitive properties so we convert them to their Object equivalents.
   *
   * Spark also has trouble de-serializing classOf[Int] because of the underlying Java classes it uses.
   */
  def serializableCopy: InputSchema = {
    this.copy(columns = columns.map(columnDef =>
      columnDef.copy(dataType = PrimitiveConverter.primitivesToObjects(columnDef.dataType))
    ))
  }
}
