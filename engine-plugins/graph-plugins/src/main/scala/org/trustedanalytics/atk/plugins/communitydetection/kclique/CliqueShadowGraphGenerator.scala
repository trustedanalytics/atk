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

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.{ VertexSet, CliqueExtension }
import org.apache.spark.rdd.RDD

object CliqueShadowGraphGenerator extends Serializable {

  /**
   * Return value of the CliqueShadowGraphGenerator
   * @param vertices List of vertices of new graph where vertices are k-cliques
   * @param edges List of edges between the vertices of new graph of k-cliques
   */
  case class CliqueShadowGraph(vertices: RDD[VertexSet],
                               edges: RDD[(VertexSet, VertexSet)])

  /**
   * Generate the clique-shadow graph from the extension facts.
   *
   *   A clique shadow graph is a bipartite graph whose vertex sets are:
   *   - The k cliques in the input graph.
   *   - All k-1 subsets of k cliques in the input graph (the "shadows" of the cliques in combinatorial parlance)
   *   There is an edge from a clique to each shadow that it contains as a subset.
   *
   * @param cliqueExtensions RDD of clique extension facts
   * @return The clique-shadow graph, packaged into a CliqueShadowGraph
   */
  def run(cliqueExtensions: RDD[CliqueExtension]) = {

    val cliques: RDD[VertexSet] = cliqueExtensions.flatMap(
      { case CliqueExtension(clique, extenders, _) => extenders.map(v => clique.members + v) })

    cliques.cache()

    val cliqueToShadowEdges: RDD[(VertexSet, VertexSet)] = cliques.flatMap(V => V.subsets(V.size - 1).map(U => (V, U)))
    cliqueToShadowEdges.cache()

    val shadows: RDD[VertexSet] = cliqueToShadowEdges.map(_._2).distinct()

    val vertices: RDD[VertexSet] = cliques.union(shadows)
    val edges: RDD[(VertexSet, VertexSet)] = cliqueToShadowEdges.flatMap({ case (x, y) => Set((x, y), (y, x)) })

    cliques.unpersist(blocking = false)
    cliqueToShadowEdges.unpersist(blocking = false)

    new CliqueShadowGraph(vertices, edges)
  }

}
