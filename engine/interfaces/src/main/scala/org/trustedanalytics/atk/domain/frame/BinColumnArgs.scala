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
 * Represents a BinColumn object
 */
case class BinColumnArgs(frame: FrameReference,
                         @ArgDoc("""Name of the column to bin.""") columnName: String,
                         @ArgDoc("""Array of values containing bin cutoff points.
Array can be list or tuple.
Array values must be progressively increasing.
All bin boundaries must be included, so, with N bins, you need N+1 values.""") cutoffs: List[Double],
                         @ArgDoc("""Specify how the boundary conditions are handled.
``True`` indicates that the lower bound of the bin is inclusive.
``False`` indicates that the upper bound is inclusive.
Default is ``True``.""") includeLowest: Option[Boolean],
                         @ArgDoc("""Specify how values outside of the cutoffs array should be binned.
If set to ``True``, each value less than cutoffs[0] or greater than
cutoffs[-1] will be assigned a bin value of -1.
If set to ``False``, values less than cutoffs[0] will be included in the first
bin while values greater than cutoffs[-1] will be included in the final
bin.""") strictBinning: Option[Boolean],
                         @ArgDoc("""The name for the new binned column.
Default is ``<column_name>_binned``.""") binColumnName: Option[String]) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(cutoffs.size >= 2, "at least one bin is required")
  require(cutoffs == cutoffs.sorted, "the cutoff points of the bins must be monotonically increasing")
}
