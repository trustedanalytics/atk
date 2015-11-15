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

package org.apache.spark.graphx.lib.atk.plugins

import org.apache.spark.graphx
import org.apache.spark.graphx.{ Graph, PartitionStrategy }
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * "Convergence threshold" in our system:
 * When the average change in posterior beliefs between supersteps falls below this threshold,
 * terminate. Terminate! TERMINATE!!!
 *
 */
class ClusteringCoefficientPluginTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val floatingPointEqualityThreshold: Double = 0.000000001d
  val defaultParallelism = 3 // > 1 to catch stupid parallelization bugs

  "clustering coefficient" should "not crash (and produce an empty graph) when run on an empty graph" in {

    val rawEdges = sparkContext.parallelize(Array[(Long, Long)](), defaultParallelism)
    val graph = Graph.fromEdgeTuples(rawEdges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testVertices.length shouldBe 0
    testEdges.length shouldBe 0
  }

  "clustering coefficient" should "should correctly give an isolated vertex clustering coefficient 0.0d" in {

    val vertex = sparkContext.parallelize(Array((1L, null)), defaultParallelism)
    val edges = sparkContext.parallelize(Array[graphx.Edge[Null]](), defaultParallelism)
    val graph = Graph(vertex, edges).partitionBy(PartitionStrategy.RandomVertexCut) cache ()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 0
    testVertices.length shouldBe 1

    val testVertexClusteringCoefficient: Double = testVertices(0)._2

    Math.abs(testVertexClusteringCoefficient - 0.0d) should be < floatingPointEqualityThreshold
    Math.abs(testGCC - 0.0d) should be < floatingPointEqualityThreshold
  }

  "clustering coefficient" should "give every vertex a CC 1.0d when analyzing a complete graph" in {

    val edges = sparkContext.parallelize(Array[(Long, Long)](1L -> 2L, 1L -> 3L, 1L -> 4L, 2L -> 3L, 2L -> 4L, 3L -> 4L), defaultParallelism)
    val graph = Graph.fromEdgeTuples(edges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 6
    testVertices.length shouldBe 4

    Math.abs(testVertices(0)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(1)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(2)._2 - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertices(3)._2 - 1.0d) should be < floatingPointEqualityThreshold

    Math.abs(testGCC - 1.0d) should be < floatingPointEqualityThreshold
  }

  "clustering coefficient" should "give correct answer on a simple four node graph " in {

    /*
     * The vertex set of the graph is 1, 2, 3, 4
     * The edge set of the graph is 12, 13, 14, 34
     * A simple inspection reveals that the local clustering coefficients are:
     * vertex 1 has CC 0.333333333333333d
     * vertex 2 has CC 0.0d
     * vertex 3 has CC 1.0d
     * vertex 4 has CC 1.0d
     */
    val edges = sparkContext.parallelize(Array[(Long, Long)](1L -> 2L, 1L -> 3L, 1L -> 4L, 3L -> 4L), defaultParallelism)
    val graph = Graph.fromEdgeTuples(edges, true).partitionBy(PartitionStrategy.RandomVertexCut).cache()
    val (testGraph, testGCC) = ClusteringCoefficient.run(graph)
    val testVertices = testGraph.vertices.collect()
    val testEdges = testGraph.edges.collect()

    testEdges.length shouldBe 4
    testVertices.length shouldBe 4

    val testVertexMap = testVertices.toMap

    Math.abs(testVertexMap(1) - 0.33333333333d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(2) - 0.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(3) - 1.0d) should be < floatingPointEqualityThreshold
    Math.abs(testVertexMap(4) - 1.0d) should be < floatingPointEqualityThreshold

    Math.abs(testGCC - 0.6d) should be < floatingPointEqualityThreshold
  }
}
