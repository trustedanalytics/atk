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
package org.trustedanalytics.atk.engine.daal.plugins

import com.intel.daal.algorithms.{ Result, PartialResult }
import com.intel.daal.services.DaalContext
import org.apache.spark.rdd.RDD

trait DistributedAlgorithm[P <: PartialResult, R <: Result] {

  def computePartialResults(): RDD[P]

  def mergePartialResults(daalContext: DaalContext, rdd: RDD[P]): R

}
