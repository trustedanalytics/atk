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

package org.trustedanalytics.atk.giraph.plugins.model.lda

import org.apache.spark.rdd._

import scala.reflect.ClassTag

/**
 * Renames the vertices of a graph from some arbitrary type T (that provides a ClassTag for Spark's benefit)
 * to Long IDs.
 *
 * @tparam T type of the vertex IDs in the incoming graph
 */

class LdaGraphIDAssigner() extends Serializable {

  /**
   * Rename the vertices of the incoming graph from IDs of type T to Longs
   * @param inVertices vertex list of incoming graph
   * @return GraphIDAssignerOutput
   */
  def assignVertexId[T: ClassTag](inVertices: RDD[T]): RDD[(Long, T)] = {

    val verticesGroupedByHashCodes = inVertices.map(v => (v.hashCode(), v)).groupBy(_._1).map(p => p._2)

    val hashGroupsWithPositions = verticesGroupedByHashCodes.flatMap(seq => seq.zip(1 to seq.size))

    val verticesWithLongIds = hashGroupsWithPositions.map(
      { case ((hashCode, vertex), bucketPosition) => ((hashCode.toLong << 32) + bucketPosition.toLong, vertex) })

    verticesWithLongIds
  }

}
