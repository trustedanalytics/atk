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


package org.trustedanalytics.atk.plugins.connectedcomponents

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd._

/**
 * Normalizes the vertex-to-connected components mapping so that community IDs come from a range
 * 1 to # of connected components (rather than simply having distinct longs as the IDs of the components).
 */
object NormalizeConnectedComponents {

  /**
   *
   * @param vertexToCCMap Map of each vertex ID to its component ID.
   * @return Pair consisting of number of connected components
   */
  def normalize(vertexToCCMap: RDD[(Long, Long)], sc: SparkContext): (Long, RDD[(Long, Long)]) = {

    // TODO implement this procedure properly when the number of connected components is enormous
    // it certainly makes sense to run this when the number of connected components requires a cluster to be stored
    // as an RDD of longs (say, if the input was many billions of disconnected nodes...)
    // BUT that may not be a sensible use case for a long time to come... and the code to handle that is significantly
    // more complicated.
    // FOR NOW... we  use the artificial restriction that there are at most 10 million connected components
    // if start tripping on that, we do this in a distributed fashion...

    val baseComponentRDD = vertexToCCMap.map(x => x._2).distinct()
    val count = baseComponentRDD.count()

    require(count < 10000000,
      "NormalizeConnectedComponents: Failed assumption: The number of connected components exceeds ten million."
        + "Please consider a fully distributed implementation of ID normalization. Thank you.")

    val componentArray = baseComponentRDD.toArray()
    val range = 1.toLong to count.toLong

    val zipped = componentArray.zip(range)
    val zippedAsRDD = sc.parallelize(zipped)

    val outRDD = vertexToCCMap.map(x => (x._2, x._1)).join(zippedAsRDD).map(x => x._2)
    (count, outRDD)
  }
}
