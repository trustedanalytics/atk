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

package org.trustedanalytics.atk.domain.frame.partitioning

import org.trustedanalytics.atk.domain.frame.FrameReference

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/** Arguments to CoalescePlugin (see Spark API) */
case class CoalesceArgs(frame: FrameReference,
                        @ArgDoc("""number of Spark RDD partitions""") numberPartitions: Int,
                        @ArgDoc("""shuffle data between partitions""") shuffle: Option[Boolean] = Some(false))
