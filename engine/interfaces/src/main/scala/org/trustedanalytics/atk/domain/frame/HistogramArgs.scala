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
 * Arguments needed to compute a histogram from a column
 */
case class HistogramArgs(frame: FrameReference,
                         @ArgDoc("""Name of column to be evaluated.""") columnName: String,
                         @ArgDoc("""Number of bins in histogram.
Default is Square-root choice will be used
(in other words math.floor(math.sqrt(frame.row_count)).""") numBins: Option[Int],
                         @ArgDoc("""Name of column containing weights.
Default is all observations are weighted equally.""") weightColumnName: Option[String],
                         @ArgDoc("""The type of binning algorithm to use: ["equalwidth"|"equaldepth"]
Defaults is "equalwidth".""") binType: Option[String] = Some("equalwidth")) {
  require(binType.isEmpty || binType == Some("equalwidth") || binType == Some("equaldepth"), "bin type must be 'equalwidth' or 'equaldepth', not " + binType)
  if (numBins.isDefined)
    require(numBins.get > 0, "the number of bins must be greater than 0")
}
