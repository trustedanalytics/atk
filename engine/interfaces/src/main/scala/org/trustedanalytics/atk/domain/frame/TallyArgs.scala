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
 * Represents a CumulativeCount object
 *
 */

case class TallyArgs(frame: FrameReference,
                     @ArgDoc("""The name of the column from which to compute the cumulative count.""") sampleCol: String,
                     @ArgDoc("""The column value to be used for the counts.""") countVal: String) {
  require(frame != null, "frame is required")
  require(sampleCol != null, "column name for sample is required")
  require(countVal != null, "count value for the sample is required")
}
