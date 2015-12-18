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

import org.trustedanalytics.atk.graphbuilder.driver.spark.elements.{ Property, GBEdge, GBVertex }
import org.scalatest.{ Matchers, FlatSpec }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.plugins.communitydetection.kclique.datatypes.Clique
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test checks that end-to-end run of k-clique percolation works with k = 3 on a small graph consisting of
 * two overlapping three-clique communities, a third non-overlapping three-clique community, and a leaf vertex that
 * belongs to no three-clique communities.
 *
 */
class StartToFinishCliqueSizeThreeTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliquePropertyNames {
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val communityProperty = "communities"
  }

  "Three clique community analysis" should "create communities according to expected equivalance classes" in new KCliquePropertyNames {

    /*
    The graph has vertex set 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 and edge set 01, 12, 13, 23, 34, 24, 45, 46, 56, 78, 89, 79

    So that the 3-clique communities are:
    {1, 2, 3, 4}. {4 ,5 6}, {7, 8, 9} ... note that 0 belongs to no 3-clique community
    */

    val edgeSet: Set[(Long, Long)] = Set((0, 1), (1, 2), (1, 3), (2, 3), (2, 4), (3, 4), (2, 3), (4, 5), (4, 6), (5, 6), (7, 8), (7, 9), (8, 9))
      .map({ case (x, y) => (x.toLong, y.toLong) }).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val vertexSet: Set[Long] = Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val inVertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val inEdgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (outVertices, outEdges) = KCliquePercolationRunner.run(inVertexRDD, inEdgeRDD, 3, communityProperty)

    val outEdgesSet = outEdges.collect().toSet
    val outVertexSet = outVertices.collect().toSet

    outEdgesSet shouldBe gbEdgeSet

    val testVerticesToCommunities = outVertexSet.map(v => (v.gbId.value.asInstanceOf[Long],
      v.getProperty(communityProperty).get.value.asInstanceOf[String])).toMap

    // vertex 0 gets no community (poor lonley little guy)
    testVerticesToCommunities(0) should be('empty)

    // vertices 1, 2, 3 each have only one community and it should be the same one
    testVerticesToCommunities(2) shouldBe testVerticesToCommunities(1)
    testVerticesToCommunities(3) shouldBe testVerticesToCommunities(1)

    // vertices 5 and 6 each have only one community and it should be the same one
    testVerticesToCommunities(5) shouldBe testVerticesToCommunities(6)

    // vertices 7, 8, 9 each have only one community and it should be the same one
    testVerticesToCommunities(7) shouldBe testVerticesToCommunities(8)
    testVerticesToCommunities(7) shouldBe testVerticesToCommunities(9)

  }

}
