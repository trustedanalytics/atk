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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Command for retrieving the top (or bottom) K distinct values by count for a specified column.
 *
 */
case class TopKArgs(@ArgDoc("""Reference to the input data frame.""") frame: FrameReference,
                    @ArgDoc("""The column whose top (or bottom) K distinct values are
to be calculated.""") columnName: String,
                    @ArgDoc("""Number of entries to return (If k is negative, return bottom k).""") k: Int,
                    @ArgDoc("""The column that provides weights (frequencies) for the topK calculation.
Must contain numerical data.
Default is 1 for all items.""") weightsColumn: Option[String] = None) {
  require(frame != null, "frame is required")
  require(columnName != null, "column name is required")
  require(k != 0, "k should not be equal to zero")
}
