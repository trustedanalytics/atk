/**
 *  Copyright (c) 2016 Intel Corporation 
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

/**
 * Represents the required parameters for bin_equal_width or bin_equal_depth
 *
 */
case class ComputedBinColumnArgs(@ArgDoc("""Identifier for the input dataframe.""") frame: FrameReference,
                                 @ArgDoc("""The column whose values are to be binned.""") columnName: String,
                                 @ArgDoc("""The maximum number of bins.
Default is the Square-root choice
:math:`\lfloor \sqrt{m} \rfloor`, where :math:`m` is the number of rows.""") numBins: Option[Int],
                                 @ArgDoc("""The name for the new column holding the grouping labels.
Default is ``<column_name>_binned``.""") binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(numBins.isEmpty || numBins.getOrElse(-1) > 0, "at least one bin is required")
}
