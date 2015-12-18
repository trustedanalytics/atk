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

import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex }

/**
 * Class to set the vertex Ids as required by Graph Builder, by formatting as (physicalId, gbId, propertyList)
 * @param gbVertices graph builder vertices list of the input graph
 * @param vertexCommunitySet pair of vertex Id and set of communities
 */
class GBVertexRddBuilder(gbVertices: RDD[GBVertex], vertexCommunitySet: RDD[(Long, Set[Long])]) extends Serializable {

  /**
   * Set the vertex as required by graph builder with new community property
   * @param communityPropertyLabel the label of the community property (provided by user)
   * @return RDD of graph builder Vertices having new community property
   */
  def setVertex(communityPropertyLabel: String): RDD[GBVertex] = {

    val emptySet: Set[Long] = Set()

    // Map the GB Vertex IDs to key-value pairs where the key is the GB Vertex ID set and the value is the emptySet.
    // The empty set will be considered as the empty communities for the vertices that don't belong to any communities.
    val gbVertexIDEmptySetPairs: RDD[(Long, Set[Long])] = gbVertices
      .map((v: GBVertex) => v.physicalId.asInstanceOf[Long])
      .map(id => (id, emptySet))

    // Combine the vertices having communities with the vertices having no communities together, to have
    // the complete list of vertices
    val gbVertexIdCombinedWithEmptyCommunity: RDD[(Long, Set[Long])] =
      gbVertexIDEmptySetPairs.union(vertexCommunitySet).combineByKey(
        x => x,
        { case (x, y) => y.union(x) },
        { case (x, y) => y.union(x) })

    // Convert the pair of vertex and community set into a GB Vertex
    val newGBVertices: RDD[GBVertex] = gbVertexIdCombinedWithEmptyCommunity.map({
      case (vertexId, communitySet) => GBVertex(
        java.lang.Long.valueOf(vertexId),
        Property("vertexId", vertexId),
        Set(Property(communityPropertyLabel, communitySet.mkString(","))))

    })
    newGBVertices
  }

}
