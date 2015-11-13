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
import org.scalatest.{ Matchers, FlatSpec, FunSuite }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class VertexCommunityAssignerTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait kCliqueVertexWithCommunityListTest {

    /*
     The graph has vertex set 1, 2, 3, 4, 5, 6, 7
     and edge set 12, 13, 14, 23,24, 34, 35, 36, 37, 46, 46, 47, 56, 57

     so there are three 4-cliques:  1234, 3456, and 3457

     1234 is in its own 4-clique community
     3456 and 3457 are in another 4-clique community
     */

    val clique1: VertexSet = Set(1, 2, 3, 4).map(_.toLong)
    val clique2: VertexSet = Set(3, 4, 5, 6).map(_.toLong)
    val clique3: VertexSet = Set(3, 4, 5, 7).map(_.toLong)

    val cliquesToCommunities = Seq(Pair(clique1, 1.toLong), Pair(clique2, 2.toLong), Pair(clique3, 2.toLong))

  }

  "Assignment of communities to the vertices" should
    "produce the pair of original k-clique vertex and set of communities it belongs to" in new kCliqueVertexWithCommunityListTest {

      val cliquesToCommunitiesRDD = sparkContext.parallelize(cliquesToCommunities)

      val verticesToCommunities = VertexCommunityAssigner.run(cliquesToCommunitiesRDD)

      val vertexToCommunitiesMap = verticesToCommunities.collect().toMap

      vertexToCommunitiesMap(1.toLong) should be(Set(1.toLong))
      vertexToCommunitiesMap(2.toLong) should be(Set(1.toLong))

      vertexToCommunitiesMap(3.toLong) should be(Set(1.toLong, 2.toLong))
      vertexToCommunitiesMap(4.toLong) should be(Set(1.toLong, 2.toLong))

      vertexToCommunitiesMap(5.toLong) should be(Set(2.toLong))
      vertexToCommunitiesMap(6.toLong) should be(Set(2.toLong))
      vertexToCommunitiesMap(7.toLong) should be(Set(2.toLong))

    }
}
