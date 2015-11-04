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
import org.apache.commons.lang.StringUtils

/**
 * Command to unflatten a frame and store the result to a new data frame.
 */
case class UnflattenColumnArgs(frame: FrameReference,
                               @ArgDoc("""Name of the column(s) to be used as keys
for unflattening.""") columns: List[String],
                               @ArgDoc("""Separator for the data in the result columns.
Default is comma (,).""") delimiter: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columns != null && columns.nonEmpty, "column list is required for key")
  columns.foreach(x => require(StringUtils.isNotBlank(x), "non empty column names required for composite key"))
}
