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


package org.trustedanalytics.atk.plugins.graphstatistics

import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Tests the calculation of undirected degrees on a simple undirected graph.
 *
 * The graph has 5 vertices and two edge relations, "A edges" and "B edges"
 *
 * The vertex set is 1, 2, 3, 4, 5.
 *
 * All edges are undirected.
 * The default weight used for a missing weight property is 0.5d
 *
 * The A edges are:  12 (weight 0.1d) , 13 (weight 0.01d) , 23 (weight 0.001d), 34 (weight missing)
 * The B edges are: 15 (weight 0.0001d), 34 (weight 0.00001d), 35 (weight 0.000001d), 45 (weight missing)
 *
 * To save you the tedium, here are the unweighted / weighted degrees of the vertices:
 *
 * Using only A edges:
 * vertex 1: 2 / 0.110
 * vertex 2: 2 / 0.101
 * vertex 3: 3 / 0.511
 * vertex 4: 1 / 0.5
 * vertex 5: 0 / 0.0
 *
 * Using only B edges:
 * vertex 1: 1 / 0.0001
 * vertex 2: 0 / 0.0
 * vertex 3: 2 / 0.000011
 * vertex 4: 2 / 0.50001
 * vertex 5: 3 /  0.501001
 *
 * Using both A and B edges:
 * vertex 1: 3 / 0.1101
 * vertex 2: 2 / 0.101
 * vertex 3: 5 / 0.511011
 * vertex 4: 3 / 1.00001
 * vertex 5: 3 / 0.501001
 *
 */
class DegreeSmallUndirectedTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of parellelism > 1 to catch stupid parallelization bugs
  val floatingPointEqualityThreshold: Double = 0.000000001d

  trait UndirectedGraphTest {

    // helper class for setting up the test data
    case class WeightedEdge(src: Long, dst: Long, weight: Double)

    val edgeLabelA = "A"
    val edgeLabelB = "B"
    val weightProperty = "weight property"
    val weightPropertyOption = Some(weightProperty)

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val defaultWeight = 0.5d

    val vertexIdList: List[Long] = List(1L, 2L, 3L, 4L, 5L)

    // for purposes of encoding, missing weights are denoted as -1.0d, see the function getPropertiesFromWeight below

    val edgeListA: List[WeightedEdge] = List(WeightedEdge(1, 2, 0.1d), WeightedEdge(1, 3, 0.01d), WeightedEdge(2, 3, 0.001d),
      WeightedEdge(3, 4, -1d))

    val aDegreeMap: Map[Long, Long] = Map((1L, 2L), (2L, 2L), (3L, 3L), (4L, 1L), (5L, 0L))
    val aWeightedDegreeMap: Map[Long, Double] = Map((1L, 0.11d), (2L, 0.101d), (3L, 0.511d), (4L, 0.5d), (5L, 0.0d))

    val edgeListB: List[WeightedEdge] = List(WeightedEdge(1, 5, 0.0001d), WeightedEdge(3, 4, 0.00001d),
      WeightedEdge(3, 5, 0.000001d), WeightedEdge(4, 5, -1d))

    val bDegreeMap: Map[Long, Long] = Map((1L, 1L), (2L, 0L), (3L, 2L), (4L, 2L), (5L, 3L))
    val bWeightedDegreeMap: Map[Long, Double] = Map((1L, 0.0001d), (2L, 0.0d), (3L, 0.000011d),
      (4L, 0.50001d), (5L, 0.500101d))

    val combinedDegreesMap: Map[Long, Long] = Map((1L, 3L), (2L, 2L), (3L, 5L), (4L, 3L), (5L, 3L))
    val combinedWeightedDegreesMap: Map[Long, Double] = Map((1L, 0.1101d), (2L, 0.101d), (3L, 0.511011d),
      (4L, 1.00001d), (5L, 0.500101d))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    private def getPropertiesFromWeight(weight: Double) = {
      if (weight >= 0) {
        Set(Property(weightProperty, weight))
      }
      else {
        Set[Property]()
      }
    }

    val gbEdgeListA =
      edgeListA.flatMap({
        case (WeightedEdge(src, dst, weight)) => Set(
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelA, getPropertiesFromWeight(weight)),
          GBEdge(None, dst, src, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelA, getPropertiesFromWeight(weight))
        )
      })

    val gbEdgeListB =
      edgeListB.flatMap({
        case (WeightedEdge(src, dst, weight)) => Set(
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelB, getPropertiesFromWeight(weight)),
          GBEdge(None, dst, src, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            edgeLabelB, getPropertiesFromWeight(weight))
        )
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeListA.union(gbEdgeListB))

    val excpectedADegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, aDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedBDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, bDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedCombinedDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, combinedDegreesMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedAWeightedDegrees: Set[(GBVertex, Double)] =
      gbVertexList.map(v => (v, aWeightedDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedBWeightedDegrees: Set[(GBVertex, Double)] =
      gbVertexList.map(v => (v, bWeightedDegreeMap(v.physicalId.asInstanceOf[Long]))).toSet

    val expectedCombinedWeightedDegrees: Set[(GBVertex, Double)] =
      gbVertexList.map(v => (v, combinedWeightedDegreesMap(v.physicalId.asInstanceOf[Long]))).toSet

    val allZeroDegrees: Set[(GBVertex, Long)] =
      gbVertexList.map(v => (v, 0L)).toSet

    val allZeroDegreesDouble: Set[(GBVertex, Double)] =
      gbVertexList.map(v => (v, 0.0d)).toSet
  }

  "simple undirected graph" should "have correct degrees for edge label A" in new UndirectedGraphTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(edgeLabelA)))
    results.collect().toSet shouldEqual excpectedADegrees
  }

  "simple undirected graph" should "have correct degrees for edge label B" in new UndirectedGraphTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(edgeLabelB)))
    results.collect().toSet shouldEqual expectedBDegrees
  }

  "simple undirected graph" should "have correct degrees when both edge label A and B requested" in new UndirectedGraphTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(edgeLabelA, edgeLabelB)))
    results.collect().toSet shouldEqual expectedCombinedDegrees
  }

  "simple undirected graph" should "have net degrees for all labels combined when edge labels are unrestricted " in new UndirectedGraphTest {
    val results = UnweightedDegrees.undirectedDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedCombinedDegrees
  }

  "simple undirected graph" should "have all degrees 0 when restricted to empty list of labels" in new UndirectedGraphTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set()))
    results.collect().toSet shouldEqual allZeroDegrees
  }

  "simple undirected graph" should "have correct weighted degrees for edge label A" in new UndirectedGraphTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(edgeLabelA)))
    val test = approximateMapEquality(results.collect().toMap, expectedAWeightedDegrees.toMap)
    test shouldBe true
  }

  "simple undirected graph" should "have correct weighted degrees for edge label B" in new UndirectedGraphTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(edgeLabelB)))
    val test = approximateMapEquality(results.collect().toMap, expectedBWeightedDegrees.toMap)
    val x = results.collect().toMap
    test shouldBe true
  }

  "simple undirected graph" should "have correct weighted degrees when both edge label A and B requested" in new UndirectedGraphTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set(edgeLabelA, edgeLabelB)))
    results.collect().toSet shouldEqual expectedCombinedWeightedDegrees
  }

  "simple undirected graph" should "have net weighted degrees for all labels combined when edge labels are unrestricted " in new UndirectedGraphTest {
    val results = WeightedDegrees.undirectedWeightedDegree(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight)
    results.collect().toSet shouldEqual expectedCombinedWeightedDegrees
  }

  "simple undirected graph" should "have all weighted degrees 0 when restricted to empty list of labels" in new UndirectedGraphTest {
    val results = WeightedDegrees.undirectedWeightedDegreeByEdgeLabel(vertexRDD, edgeRDD, weightPropertyOption, defaultWeight, Some(Set()))
    results.collect().toSet shouldEqual allZeroDegreesDouble
  }

  private def approximateMapEquality(map1: Map[GBVertex, Double], map2: Map[GBVertex, Double]): Boolean = {
    (map1.keySet equals map2.keySet) && map1.keySet.forall(k => Math.abs(map1.apply(k) - map2.apply(k)) < floatingPointEqualityThreshold)
  }
}
