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

package org.trustedanalytics.atk.pregel

import org.apache.spark.rdd.RDD

case class SuperStepStatus(log: String, earlyTermination: Boolean)

/**
 * Implementations of this trait provide a method for generating a summary of superstep activity after the completion
 * of a Pregel superstep.
 * @tparam V Class of the vertex data.
 */
trait SuperStepStatusGenerator[V] extends Serializable {
  def generateSuperStepStatus(iteration: Int, totalVertices: Long, activeVertices: RDD[V]): SuperStepStatus
}
