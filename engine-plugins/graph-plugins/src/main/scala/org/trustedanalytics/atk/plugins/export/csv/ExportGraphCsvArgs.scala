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

package org.trustedanalytics.atk.plugins.export.csv

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Input arguments class for export to CSV
 */
//TODO: update doc

case class ExportGraphCsvArgs(graph: GraphReference,
                              @ArgDoc("""The HDFS folder path where the files
will be created.""") folderName: String,
                              @ArgDoc("""The separator for separating the values.
Default is comma (,).""") separator: String = ",",
                              @ArgDoc("""The number of records you want.
Default, or a non-positive value, is the whole frame.""") count: Int = -1,
                              @ArgDoc("""The number of rows to skip before exporting to the file.
Default is zero (0).""") offset: Int = 0) {
  require(graph != null, "graph is required")
  require(folderName != null, "folder name is required")
  require(StringUtils.isNotBlank(separator) && separator.length == 1, "A single character separator is required")
}

