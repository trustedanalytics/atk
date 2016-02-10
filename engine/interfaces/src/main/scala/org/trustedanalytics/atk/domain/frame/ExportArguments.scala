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

package org.trustedanalytics.atk.domain.frame

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Input arguments class for export to CSV
 */

case class ExportHdfsCsvArgs(frame: FrameReference,
                             @ArgDoc("""The HDFS folder path where the files
will be created.""") folderName: String,
                             @ArgDoc("""The separator for separating the values.
Default is comma (,).""") separator: String = ",",
                             @ArgDoc("""The number of records you want.
Default, or a non-positive value, is the whole frame.""") count: Int = -1,
                             @ArgDoc("""The number of rows to skip before exporting to the file.
Default is zero (0).""") offset: Int = 0) {
  require(frame != null, "frame is required")
  require(folderName != null, "folder name is required")
  require(StringUtils.isNotBlank(separator) && separator.length == 1)
}

/**
 * Input arguments class for export to JSON
 */
case class ExportHdfsJsonArgs(frame: FrameReference,
                              @ArgDoc("""The HDFS folder path where the files
will be created.""") folderName: String,
                              @ArgDoc("""The number of records you want.
Default (0), or a non-positive value, is the whole frame.""") count: Int = 0,
                              @ArgDoc("""The number of rows to skip before exporting to the file.
Default is zero (0).""") offset: Int = 0) {
  require(frame != null, "frame is required")
  require(folderName != null, "folder name is required")
}

/**
 * Input arguments class for export to Hive
 */
case class ExportHdfsHiveArgs(frame: FrameReference,
                              @ArgDoc("The name of the Hive table that will contain the exported frame") tableName: String) {
  require(frame != null, "frame is required")
  require(tableName != null, "table name is required")
}

/**
 * Input arguments class for export to HBase
 */
case class ExportHdfsHBaseArgs(@ArgDoc("Frame being exported to HBase") frame: FrameReference,
                               @ArgDoc("The name of the HBase table that will contain the exported frame") tableName: String,
                               @ArgDoc("The name of the column to be used as row key in hbase table") keyColumnName: Option[String],
                               @ArgDoc("The family name of the HBase table that will contain the exported frame") familyName: String = "familyColumn") {
  require(frame != null, "frame is required")
  require(tableName != null, "table name is required")
}

/**
 * Input arguments class for export to JDBC
 */
case class ExportHdfsJdbcArgs(@ArgDoc("""Frame to be exported to JDBC""") frame: FrameReference,
                              @ArgDoc("""JDBC table name""") tableName: String,
                              @ArgDoc("""(optional) JDBC connector type""") connectorType: Option[String],
                              @ArgDoc("""(optional) driver name""") driverName: Option[String],
                              @ArgDoc("""(optional) query for filtering. Not supported yet.""") query: Option[String] = None) {
  require(frame != null, "frame is required")
  require(StringUtils.isNotEmpty(tableName), "table name is required")
}
