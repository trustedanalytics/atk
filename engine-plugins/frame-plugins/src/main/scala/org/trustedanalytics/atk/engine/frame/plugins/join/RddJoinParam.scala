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

package org.trustedanalytics.atk.engine.frame.plugins.join

import org.apache.spark.frame.FrameRdd

/**
 * Join parameters for RDD
 *
 * @param frame Frame used for join
 * @param joinColumn Join column name
 * @param estimatedSizeInBytes Optional estimated size of RDD in bytes used to determine whether to use a broadcast join
 */
case class RddJoinParam(frame: FrameRdd,
                        joinColumn: String,
                        estimatedSizeInBytes: Option[Long] = None) {
  require(frame != null, "join frame is required")
  require(joinColumn != null, "join column is required")
  require(estimatedSizeInBytes.isEmpty || estimatedSizeInBytes.get > 0,
    "Estimated rdd size in bytes should be empty or greater than zero")
}
