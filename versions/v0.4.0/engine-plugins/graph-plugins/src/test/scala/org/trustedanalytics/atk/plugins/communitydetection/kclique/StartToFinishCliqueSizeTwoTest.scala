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

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test checks that end-to-end run of k-clique percolation works with k = 2 on a small graph consisting of
 * two non-trivial connected components and an isolated vertex.
 *
 * It is intended as "base case" functionality.
 */

class StartToFinishCliqueSizeTwoTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait KCliquePropertyNames {
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val communityProperty = "communities"
  }

  "Two clique community analysis" should "create communities according to expected equivalance classes" in new KCliquePropertyNames {

    /*
   The graph has vertex set 1, 2, 3 , 4, 5, 6, 7 and edge set 13, 35, 24, 26, 46
   So that the 2-clique communities (the connected components that are not isolated vertices) are going to be:
   {1, 3, 5},  {2, 4, 6},  Note that 7 belongs to no 2 clique community.
   */

    val edgeSet: Set[(Long, Long)] = Set((1, 3), (3, 5), (2, 4), (2, 6), (4, 6))
      .map({ case (x, y) => (x.toLong, y.toLong) }).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5, 6, 7)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val inVertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val inEdgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (outVertices, outEdges) = KCliquePercolationRunner.run(inVertexRDD, inEdgeRDD, 2, communityProperty)

    val outEdgesSet = outEdges.collect().toSet
    val outVertexSet = outVertices.collect().toSet

    outEdgesSet shouldBe gbEdgeSet

    val testVerticesToCommunities = outVertexSet.map(v => (v.gbId.value.asInstanceOf[Long],
      v.getProperty(communityProperty).get.value.asInstanceOf[String])).toMap

    // vertex 7 gets no community (poor lonley little guy)
    testVerticesToCommunities(7) should be('empty)

    // no vertex gets more than one two-clique community (this is a general property of two-clique communities...
    // they're just the connected components of the non-isolated vertices)

    // vertex 1 and vertex 2 get distinct two-clique communities
    testVerticesToCommunities(1) should not be testVerticesToCommunities(2)

    // vertex 3 and vertex 5 have the same two-clique community as vertex 1
    testVerticesToCommunities(3) shouldBe testVerticesToCommunities(1)
    testVerticesToCommunities(5) shouldBe testVerticesToCommunities(1)

    // vertex 4 and vertex 6 have the same two-clique community as vertex 2
    testVerticesToCommunities(4) shouldBe testVerticesToCommunities(2)
    testVerticesToCommunities(6) shouldBe testVerticesToCommunities(2)
  }

}
