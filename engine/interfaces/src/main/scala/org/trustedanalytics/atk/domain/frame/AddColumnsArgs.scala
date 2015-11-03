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

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

case class AddColumnsArgs(
    @ArgDoc("Frame to which new columns need to be added") frame: FrameReference,
    @ArgDoc("List of names for the new columns") columnNames: List[String],
    @ArgDoc("List of data types for the new columns") columnTypes: List[String],
    @ArgDoc("""User-Defined Function (|UDF|) which takes the values in the row
and produces a value, or collection of values, for the
new cell(s).""") udf: Udf,
    @ArgDoc("""List of columns which the |UDF| will access.
This adds significant performance benefit if we know which column(s) will be
needed to execute the |UDF|, especially when the frame has significantly more
columns than those being used to evaluate
the |UDF|.""") columnsAccessed: List[String]) {
  require(frame != null, "frame is required")
  require(columnNames != null, "column names is required")
  for {
    i <- columnNames.indices
  } {
    require(columnNames(i) != "", "column name is required")
  }
  require(columnTypes != null, "column types is required")
  require(columnNames.size == columnTypes.size, "Equal number of column names and types is required")
  require(udf != null, "User defined expression is required")
}
