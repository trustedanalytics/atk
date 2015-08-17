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

package org.trustedanalytics.atk.engine.frame.plugins.sortedk

import org.trustedanalytics.atk.domain.frame.FrameReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments for SortedK plugin
 */
case class SortedKArgs(@ArgDoc("""Frame to sort.""") frame: FrameReference,
                       @ArgDoc("""Number of sorted records to return.""") k: Int,
                       @ArgDoc("""Column names to sort by, and true to sort column by ascending order,
or false for descending order.""") columnNamesAndAscending: List[(String, Boolean)],
                       @ArgDoc("""Advanced tuning parameter which determines the depth of the
reduce-tree for the sorted_k plugin.
This plugin uses Spark's treeReduce() for scalability.
The default depth is 2.""") reduceTreeDepth: Option[Int] = None) {
  require(frame != null, "frame is required")
  require(k > 0, "k should be greater than zero") //TODO: Should we add an upper bound for K
  require(columnNamesAndAscending != null && columnNamesAndAscending.nonEmpty, "one or more columnNames is required")
  require(reduceTreeDepth.getOrElse(1) >= 1, s"Depth of reduce tree must be greater than or equal to 1")
}
