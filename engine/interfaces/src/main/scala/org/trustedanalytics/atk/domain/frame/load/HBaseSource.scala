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


package org.trustedanalytics.atk.domain.frame.load

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Arguments for the plug-in
 * @param destination destination frame
 * @param tableName hbase table name
 * @param schema hbase schema
 * @param startTag optional start tag for filtering
 * @param endTag optional end tag for filtering
 */
case class HBaseArgs(destination: FrameReference,
                     @ArgDoc("""hbase table name""") tableName: String,
                     @ArgDoc("""hbase schema as a list of tuples (columnFamily, columnName, dataType for cell value)""") schema: List[HBaseSchemaArgs],
                     @ArgDoc("""optional start tag for filtering""") startTag: Option[String] = None,
                     @ArgDoc("""optional end tag for filtering""") endTag: Option[String] = None) {

  require(StringUtils.isNotEmpty(tableName), "database name cannot be null")
  require(schema != null, "table schema cannot be null")
}

/**
 * Arguments for the schema
 * @param columnFamily hbase column family
 * @param columnName hbase column name
 * @param dataType data type for the cell
 */
case class HBaseSchemaArgs(columnFamily: String, columnName: String, dataType: DataType)
