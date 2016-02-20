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
package org.trustedanalytics.atk.pregel

import org.apache.spark.rdd.RDD

/**
 * Implementations of this trait provide a method for creating an initial status report for a Pregel-run using the
 * incoming edge and vertex RDDs.
 * @tparam V Class of the vertex data in the graph.
 * @tparam E Class of the edge data in the graph.
 */
trait InitialReport[V, E] {
  def generateInitialReport(vertices: RDD[V], edges: RDD[E]): String
}
