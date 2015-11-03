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

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes._

/**
 * CliqueEnumerator is responsible for enumerating k-clique extension facts from (k-1) clique extension facts.
 * A k-clique extension fact is a clique extension fact where the vertex set contains exactly k vertices.
 * These are the extension facts obtained after the k'th round of the algorithm. It encodes the fact that a
 * given VertexSet forms a clique, and that the clique can be extended by adding any one of the vertices
 * from the ExtendersSet and that the clique can be extended to a k+1 clique
 */

object CliqueExtensionEnumerator {

  /**
   * Entry point of the clique enumerator. It invokes a recursive call that extends a k-clique to a (k+1)-clique
   * @param edgeList RDD of edge list of the underlying graph
   * @param cliqueSize Parameter determining the size of clique. It has been used to determine
   *                   the connected communities. The minimum value must be 2
   * @return RDD of extenders fact obtained after running 'k' number of iterations, where the value of 'k' is
   *         same as (cliqueSize - 1). The extenders fact is a pair of a (cliqueSize - 1)-Clique and the set of
   *         vertices that extend it to a cliqueSize-Clique
   */
  def run(edgeList: RDD[Edge], cliqueSize: Int): RDD[CliqueExtension] = {

    /**
     * Recursive method that extends a k-clique to a (k+1)-clique
     * @param k the iteration number of the recursive call for extending cliques
     * @return RDD of the extended (k+1) clique as an extenders fact, where the extension is a k-clique
     *         and the set of vertices extending it to form a (k+1)-clique
     */
    def cliqueExtension(k: Int): RDD[CliqueExtension] = {
      if (k == 1) {
        initialExtendByMappingFrom(edgeList)
      }
      else {
        val kMinusOneExtensionFacts = cliqueExtension(k - 1)
        kMinusOneExtensionFacts.cache()

        val kCliques = deriveKCliquesFromKMinusOneExtensions(kMinusOneExtensionFacts)
        val kNeighborsOfFacts = deriveNeighborsFromExtensions(kMinusOneExtensionFacts, k % 2 == 1)
        kMinusOneExtensionFacts.unpersist(blocking = false)

        val extensions: RDD[CliqueExtension] = deriveNextExtensionsFromCliquesAndNeighbors(kCliques, kNeighborsOfFacts)
        kCliques.unpersist(blocking = false)
        kNeighborsOfFacts.unpersist(blocking = false)

        extensions
      }
    }

    // Recursive call to extend a k-clique to a (k+1)-clique
    cliqueExtension(cliqueSize - 1)
  }

  /**
   * Derive the 1 clique-extension facts from the edge list, which means to gather
   * the neighbors of the source vertices into an adjacency list (using sets),
   * which will provide the starting point for later expansion as we add more connected
   * vertices.
   *
   * Notice that the invariant holds:
   *
   * k is odd, and every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
   *
   * @param edgeList Edge list representation of the graph.
   * @return RDD of extended-by facts.
   */

  private def initialExtendByMappingFrom(edgeList: RDD[Edge]): RDD[CliqueExtension] = {
    //A map of source vertices to a set of destination vertices connected from the source
    val initMap = edgeList.groupBy(_.source).mapValues(_.map(_.destination).toSet)

    //A map of singleton sets (containing source vertices) to the set of neighbors -
    //essentially an adjacency list
    initMap.map(vToVListMap => CliqueExtension(Clique(Set(vToVListMap._1)), vToVListMap._2, neighborsHigh = true))
  }

  /**
   * Generate all the k+1-cliques from an RDD of k-cliques and their extensions
   *
   * @param extensionFacts the k-cliques and extenders
   * @return an RDD of k+1 cliques
   */
  private def deriveKCliquesFromKMinusOneExtensions(extensionFacts: RDD[CliqueExtension]): RDD[Clique] = {
    extensionFacts.flatMap(extendClique)
  }

  /**
   * Generate all the k+1-cliques from a k-clique and a set of vertices that extend it (are connected to all
   * vertices in the k-clique)
   *
   * @param extendersFact the k-clique and the vertices that are connected to every vertex in the k-clique
   * @return a k+1 clique
   */
  private def extendClique(extendersFact: CliqueExtension): Set[Clique] = {
    extendersFact match {
      case CliqueExtension(clique, extenders, neighborHigh: Boolean) =>
        extenders.map(extendByVertex => Clique(clique.members + extendByVertex))
    }
  }

  /**
   * Derive neighbors-of facts from an extends-by fact.
   *
   * INVARIANT:
   * when verticesLessThanNeighbor, every vertex ID in the clique members is less than the vertex ID in the resulting
   * NeighborsOfFact. Otherwise, every vertex ID in the clique members is greater than the vertex ID in the resulting
   * NeighborsOfFact.
   *
   *
   * @param extensionFacts RDD of ExtendersFacts from round k-1
   * @return The neighbors-of facts for this extender fact
   */
  private def deriveNeighborsFromExtensions(extensionFacts: RDD[CliqueExtension],
                                            verticesLessThanNeighbor: Boolean): RDD[NeighborsOfFact] = {
    extensionFacts.flatMap(deriveNeighbors)
  }

  /**
   * Derive neighbors-of facts from an extends-by fact.
   *
   * INVARIANT:
   * when verticesLessThanNeighbor, every vertex ID in the clique members is less than the vertex ID in the resulting
   * NeighborsOfFact. Otherwise, every vertex ID in the clique members is greater than the vertex ID in the resulting
   * NeighborsOfFact.
   *
   *
   * @param extenderFact ExtendersFact from round k-1
   * @return The neighbors-of facts for this extender fact
   */
  private def deriveNeighbors(extenderFact: CliqueExtension): Iterator[NeighborsOfFact] = {
    extenderFact match {
      case CliqueExtension(clique, extenders: VertexSet, _) =>

        val twoSetsFromExtenders = extenders.subsets(2)

        if (extenderFact.neighborsHigh) {
          val minimumCliqueMember = clique.members.min
          twoSetsFromExtenders.map(subset =>
            NeighborsOfFact(subset ++ (clique.members - minimumCliqueMember), minimumCliqueMember, neighborHigh = false))
        }
        else {
          val maximumCliqueMember = clique.members.max
          twoSetsFromExtenders.map(subset =>
            NeighborsOfFact(subset ++ (clique.members - maximumCliqueMember), maximumCliqueMember, neighborHigh = true))
        }
    }
  }

  /**
   * Combines clique facts and neighborsof facts into extended-by facts.
   *
   * INVARIANT:
   * when k is odd, every vertex ID in the VertexSet is less than every vertex ID in the ExtendersSet.
   * when k is even, every vertex ID in the VertexSet is greater than every vertex ID in the ExtenderSet.
   *
   * This invariant is inherited from the k neighbors-of facts.
   *
   * @param cliques The set of k-cliques in the graph.
   * @param neighborFacts The set of neighbors-of facts of k-sets in the graph.
   * @return Set of (k+1) clique extension facts.
   */
  private def deriveNextExtensionsFromCliquesAndNeighbors(cliques: RDD[Clique], neighborFacts: RDD[NeighborsOfFact]): RDD[CliqueExtension] = {

    //Map cliques to key-value pairs where the key is the vertex set and the value is a 0. Don't care
    //about the zero because it's just to get us into a pair so we can call cogroup later.
    val cliquesAsPairs: RDD[(VertexSet, Int)] = cliques.map({ case Clique(members) => (members, 0) })

    //Map neighbors to key-value pairs where the key is the vertex set and the value is a pair
    //of the neighbor and the "neighborHigh" flag. These flags will be the same for every pair,
    //so a later optimization might be able to eliminate that part.
    val neighborsAsPairs = neighborFacts.map({
      case NeighborsOfFact(members, neighbor, neighborHigh) =>
        (members, (neighbor, neighborHigh))
    })

    //Cogroups (join, then group by) cliques and neighbor sets by identical vertex sets (the keys of the two RDDs
    //above). The "cliqueTags" are a Seq of zeroes. We care about that for the filter that comes next.
    val cliquesAndNeighborsCoGrouped = cliquesAsPairs.cogroup(neighborsAsPairs)
      .map({
        case (members, (cliqueTags, neighbors)) =>
          (members, cliqueTags, neighbors)
      })

    //remove vertex sets that don't have cliques or don't have neighbors - these don't make it to the next round.
    val filteredCoGroups = cliquesAndNeighborsCoGrouped.filter(
      {
        case (members, cliqueTags, neighbors) =>
          cliqueTags.nonEmpty && neighbors.nonEmpty
      })

    //Repackage these tuples as ExtenderFacts
    val cliquesAndNeighbors = filteredCoGroups.map({
      case (members, cliqueTags, neighbors) =>
        val (neighborVertices, neighborHighs) = neighbors.unzip(identity)
        CliqueExtension(Clique(members),
          neighborVertices.toSet,
          neighborHighs.head)
    })

    cliquesAndNeighbors
  }

}
