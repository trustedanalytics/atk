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
package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.{ Clique, CliqueExtension, VertexSet }
import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.Clique
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test validates that the GraphGenerator correctly constructs a clique-shadow graph as follows:
 *
 */
class CliqueShadowGraphGeneratorTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliqueGraphGenTest {

    /*
     The graph is on the vertex set 1, 2, 3, 4, 5, 6, 7, 8, 9 and the edge set is
     12, 13, 23, 24, 34, 45, 46, 56, 78, 79, 89

     so the set of triangles is
     123,  234, 456, 789

     and the 2-clique (ie. edge) shadows of the triangles are:
     12, 13, 23, 24, 34, 45, 46, 56, 78, 79, 89

     in the shadow-clique graph the edges are from the shadows to the cliques that contain them
     */

    val extensionsOfTwoCliques: Set[CliqueExtension] = Set((Set(1, 2), Set(3)), (Set(2, 3), Set(4)), (Set(4, 5), Set(6)),
      (Set(7, 8), Set(9))).map({
        case (clique, extenders) =>
          CliqueExtension(Clique(clique.map(_.toLong)), extenders.map(_.toLong), neighborsHigh = true)
      })

    val triangleVertices = Set(Set(1, 2, 3), Set(2, 3, 4), Set(4, 5, 6), Set(7, 8, 9)).map(clique => clique.map(_.toLong))

    val shadowVertices = List(Array(1, 2), Array(1, 3), Array(2, 3), Array(2, 4), Array(3, 4), Array(4, 5), Array(4, 6),
      Array(5, 6), Array(7, 8), Array(7, 9), Array(8, 9)).map(clique => clique.map(_.toLong).toSet)

    val shadowGraphVertices = triangleVertices ++ shadowVertices

    val shadowGraphEdges = Set(
      (Set(1, 2, 3), Set(1, 2)), (Set(1, 2, 3), Set(1, 3)), (Set(1, 2, 3), Set(2, 3)),
      (Set(2, 3, 4), Set(2, 3)), (Set(2, 3, 4), Set(2, 4)), (Set(2, 3, 4), Set(3, 4)),
      (Set(4, 5, 6), Set(4, 5)), (Set(4, 5, 6), Set(4, 6)), (Set(4, 5, 6), Set(5, 6)),
      (Set(7, 8, 9), Set(8, 9)), (Set(7, 8, 9), Set(7, 8)), (Set(7, 8, 9), Set(7, 9))
    ).map({ case (x, y) => (x.map(_.toLong), y.map(_.toLong)) }).flatMap({ case (x, y) => Set((x, y), (y, x)) })
  }

  "K-Clique graph" should
    "have each k-cliques as the vertex of the new graph " in new KCliqueGraphGenTest {

      val twoCliqueExtensionsRDD: RDD[CliqueExtension] = sparkContext.parallelize(extensionsOfTwoCliques.toList)

      val cliqueShadowGraphOutput = CliqueShadowGraphGenerator.run(twoCliqueExtensionsRDD)

      val testVertexSet: Set[VertexSet] = cliqueShadowGraphOutput.vertices.collect().toSet
      val testEdgeSet: Set[(VertexSet, VertexSet)] = cliqueShadowGraphOutput.edges.collect().toSet

      testVertexSet shouldBe shadowGraphVertices
      testEdgeSet shouldBe shadowGraphEdges
    }
}
