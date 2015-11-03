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
import org.trustedanalytics.atk.plugins.connectedcomponents.ConnectedComponentsGraphXDefault
import org.trustedanalytics.atk.plugins.idassigner.GraphIDAssigner
import org.trustedanalytics.atk.plugins.idassigner._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Assign new Long IDs for each K-cliques of the k-clique graphs. Create a new graph using these Long IDs as
 * new vertices and run connected components to get communities
 */

object GetConnectedComponents extends Serializable {

  /**
   * Run the connected components and get the mappings of cliques to component IDs.
   *
   * @return RDD of pairs of (clique, community ID) where each ID is the component of the clique graph to which the clique
   *         belongs.
   */
  def run(cliqueGraphVertices: RDD[VertexSet], cliqueGraphEdges: RDD[(VertexSet, VertexSet)]): RDD[(VertexSet, Long)] = {

    //    Generate new Long IDs for each K-Clique in k-clique graph. These long IDs will be the vertices
    //    of a new graph. In this new graph, the edge between two vertices will exists if the two original
    //    k-cliques corresponding to the two vertices have exactly (k-1) number of elements in common
    val graphIDAssigner = new GraphIDAssigner[VertexSet]()
    val graphIDAssignerOutput = graphIDAssigner.run(cliqueGraphVertices, cliqueGraphEdges)
    val cliqueIDsToCliques = graphIDAssignerOutput.newIdsToOld

    // Get the vertices of the k-clique graph
    val renamedVerticesOfCliqueGraph = graphIDAssignerOutput.vertices

    // Get the edges of the k-clique graph
    val renamedEdgesOfCliqueGraph = graphIDAssignerOutput.edges

    // Get the pair of the new vertex Id and the corresponding set of k-clique vertices
    val newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)] = graphIDAssignerOutput.newIdsToOld

    // Run the connected components of the new k-clique graph
    val cliqueIdToCommunityId: RDD[(Long, Long)] =
      ConnectedComponentsGraphXDefault.run(renamedVerticesOfCliqueGraph, renamedEdgesOfCliqueGraph)

    val cliqueToCommunityID: RDD[(VertexSet, Long)] = cliqueIDsToCliques.join(cliqueIdToCommunityId).map(_._2)

    cliqueToCommunityID
  }

  /**
   * Return value of connected components including the community IDs
   * @param connectedComponents pair of new vertex ID and community ID
   * @param newVertexIdToOldVertexIdOfCliqueGraph mapping between new vertex ID and original k-cliques
   */
  case class ConnectedComponentsOutput(connectedComponents: RDD[(Long, Long)], newVertexIdToOldVertexIdOfCliqueGraph: RDD[(Long, VertexSet)])

}
