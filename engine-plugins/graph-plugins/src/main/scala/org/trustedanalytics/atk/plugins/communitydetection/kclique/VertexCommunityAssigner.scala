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

package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.VertexSet
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Assign to each vertex the list of communities to which it belongs, given the assignment of cliques to communities.
 */

object VertexCommunityAssigner extends Serializable {

  /**
   * Assign to each vertex the list of communities to which it belongs, given the assignment of cliques to communities.
   *
   * @param cliquesToCommunities Mapping from cliques to the community ID of that clique.
   * @return Mapping from vertex IDs to the list of communities to which that vertex belongs.If a vertex belongs to no
   *         cliques, its list of communities will be empty.
   */
  def run(cliquesToCommunities: RDD[(VertexSet, Long)]): RDD[(Long, Set[Long])] = {

    val vertexCommunityPairs: RDD[(Long, Long)] =
      cliquesToCommunities.flatMap({ case (clique, communityID) => clique.map(v => (v, communityID)) })

    vertexCommunityPairs.groupByKey().map({ case (vertex, communitySeq) => (vertex, communitySeq.toSet) })
  }
}
