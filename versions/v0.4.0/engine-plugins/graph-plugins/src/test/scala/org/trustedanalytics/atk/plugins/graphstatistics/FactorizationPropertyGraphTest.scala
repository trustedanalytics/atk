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

package org.trustedanalytics.atk.plugins.graphstatistics

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Test the behavior of the degree calculation routines on a property graph with two distinct edge labels.
 *
 * The graph is on the integers 1 through 20 with two edges:  divisorOf and multipleOf
 * with a divisorOf edge from a to b when a | b and a multipleOf edge from a to b when b|a
 *
 */
class FactorizationPropertyGraphTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of parellelism > 1 to catch stupid parallelization bugs

  trait FactorizationPGraphTest {

    val divisorOfLabel = "divisorOf"
    val multipleOfLabel = "multipleOf"

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = (1L to 20L).toList

    val numToDivisors: Map[Long, Set[Int]] = Map(
      1L -> Set(1),
      2L -> Set(1, 2),
      3L -> Set(1, 3),
      4L -> Set(1, 2, 4),
      5L -> Set(1, 5),
      6L -> Set(1, 2, 3, 6),
      7L -> Set(1, 7),
      8L -> Set(1, 2, 4, 8),
      9L -> Set(1, 3, 9),
      10L -> Set(1, 2, 5, 10),
      11L -> Set(1, 11),
      12L -> Set(1, 2, 3, 4, 6, 12),
      13L -> Set(1, 13),
      14L -> Set(1, 2, 7, 14),
      15L -> Set(1, 3, 5, 15),
      16L -> Set(1, 2, 4, 8, 16),
      17L -> Set(1, 17),
      18L -> Set(1, 2, 3, 6, 9, 18),
      19L -> Set(1, 19),
      20L -> Set(1, 2, 4, 5, 10, 20)
    )

    val numToMultiples: Map[Long, Set[Int]] = Map(
      1L -> (1 to 20).toSet,
      2L -> Set(2, 4, 6, 8, 10, 12, 14, 16, 18, 20),
      3L -> Set(3, 6, 9, 12, 15, 18),
      4L -> Set(4, 8, 12, 16, 20),
      5L -> Set(5, 10, 15, 20),
      6L -> Set(6, 12, 18),
      7L -> Set(7, 14),
      8L -> Set(8, 16),
      9L -> Set(9, 18),
      10L -> Set(10, 20),
      11L -> Set(11),
      12L -> Set(12),
      13L -> Set(13),
      14L -> Set(14),
      15L -> Set(15),
      16L -> Set(16),
      17L -> Set(17),
      18L -> Set(18),
      19L -> Set(19),
      20L -> Set(20)
    )

    val divisorEdgeList: List[(Long, Long)] =
      numToDivisors.toList.flatMap({ case (i, divisorSet) => divisorSet.map(d => (d.toLong, i.toLong)) })

    val multiplesEdgeList: List[(Long, Long)] =
      numToMultiples.toList.flatMap({ case (i, multiples) => multiples.map(m => (m.toLong, i.toLong)) })

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbDivisorEdgeList =
      divisorEdgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            divisorOfLabel, Set.empty[Property])
      })

    val gbMultipleEdgeList =
      multiplesEdgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            multipleOfLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbDivisorEdgeList.union(gbMultipleEdgeList))

    val expectedDivisorInDegreeOutput: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, numToDivisors(v.physicalId.asInstanceOf[Long]).size.toLong)).toSet

    val expectedMultipleInDegreeOutput: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, numToMultiples(v.physicalId.asInstanceOf[Long]).size.toLong)).toSet

    val expectedAllLabelsInDegreeOutput: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, numToMultiples(v.physicalId.asInstanceOf[Long]).size.toLong
        + numToDivisors(v.physicalId.asInstanceOf[Long]).size.toLong)).toSet

    val expectedDivisorOutDegreeOutput = expectedMultipleInDegreeOutput
    val expectedMultipleOutDegreeOutput = expectedDivisorInDegreeOutput
    val expectedAllLabelsOutDegreeOutput = expectedAllLabelsInDegreeOutput

  }

  "factorization graph" should "have correct divisor in-degree" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(divisorOfLabel)))
    results.collect().toSet shouldEqual expectedDivisorInDegreeOutput
  }

  "factorization graph" should "have correct divisor out-degree" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(divisorOfLabel)))
    results.collect().toSet shouldEqual expectedDivisorOutDegreeOutput
  }

  "factorization graph" should "have correct multiple-of in-degree" in new FactorizationPGraphTest {

    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(multipleOfLabel)))
    results.collect().toSet shouldEqual expectedMultipleInDegreeOutput
  }

  "factorization graph" should "have correct multiple-of out-degree" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(multipleOfLabel)))
    results.collect().toSet shouldEqual expectedMultipleOutDegreeOutput
  }

  "factorization graph" should "have correct in-degree with all labels" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.inDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedAllLabelsInDegreeOutput
  }

  "factorization graph" should "have correct out-degree with all labels" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.outDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedAllLabelsOutDegreeOutput
  }

  "factorization graph" should "have correct in-degree with all labels as specified set" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(divisorOfLabel, multipleOfLabel)))
    results.collect().toSet shouldEqual expectedAllLabelsInDegreeOutput
  }

  "factorization graph" should "have correct out-degree with all labels as specified set" in new FactorizationPGraphTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(divisorOfLabel, multipleOfLabel)))
    results.collect().toSet shouldEqual expectedAllLabelsOutDegreeOutput
  }
}
