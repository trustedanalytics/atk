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

package org.trustedanalytics.atk.plugins.communitydetection.kclique

import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.VertexSet
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class GetConnectedComponentsTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait getConnectedComponentsTest {
    /*
     The test graph is on the vertex set 1, 2, 3, 4, 5, 6,  and the edge set is
     1-2, 2-3, 3-4, 1-3, 2-4, 4-5, 4-6, 5-6,
     so it has the cliques 123, 234, and 456

    */

    val clique1: VertexSet = Set(1.toLong, 2.toLong, 3.toLong)
    val clique2: VertexSet = Set(2.toLong, 3.toLong, 4.toLong)
    val clique3: VertexSet = Set(4.toLong, 5.toLong, 6.toLong)

    val cliqueGraphVertexSet = Seq(clique1, clique2, clique3)
    val cliqueGraphEdgeSet = Seq(Pair(clique1, clique2))

  }

  "K-Clique Connected Components" should
    "produce the same number of pairs of vertices and component ID as the number of vertices in the input graph" in new getConnectedComponentsTest {

      val vertexRDD = sparkContext.parallelize(cliqueGraphVertexSet)
      val edgeRDD = sparkContext.parallelize(cliqueGraphEdgeSet)

      val cliquesToCommunities = GetConnectedComponents.run(vertexRDD, edgeRDD)

      val cliquesToCommunitiesMap = cliquesToCommunities.collect().toMap

      cliquesToCommunitiesMap.keySet should contain(clique1)
      cliquesToCommunitiesMap.keySet should contain(clique2)
      cliquesToCommunitiesMap.keySet should contain(clique3)
    }
}
