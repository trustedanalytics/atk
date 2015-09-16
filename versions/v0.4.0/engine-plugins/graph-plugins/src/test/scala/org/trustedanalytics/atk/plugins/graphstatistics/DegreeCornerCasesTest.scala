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
 * Exercises the degree calculation utilities on trivial and malformed graphs.
 */
class DegreeCornerCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  val defaultParallelism = 3 // use of value > 1 to catch stupid parallelization bugs

  "empty graph" should "result in empty results" in {
    val vertexRDD = sparkContext.parallelize(List.empty[GBVertex], defaultParallelism)
    val edgeRDD = sparkContext.parallelize(List.empty[GBEdge], defaultParallelism)

    UnweightedDegrees.outDegrees(vertexRDD, edgeRDD).count() shouldBe 0
    UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set("edge label"))).count() shouldBe 0
    UnweightedDegrees.inDegrees(vertexRDD, edgeRDD).count() shouldBe 0
    UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set("edge label"))).count() shouldBe 0
  }

  "single node graph" should "have all edge labels degree 0" in {

    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1)
    val edgeList: List[(Long, Long)] = List()

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    val expectedOutput =
      gbVertexList.map(v => (v, 0L)).toSet

    UnweightedDegrees.outDegrees(vertexRDD, edgeRDD).collect().toSet shouldBe expectedOutput
    UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
    UnweightedDegrees.inDegrees(vertexRDD, edgeRDD).collect().toSet shouldBe expectedOutput
    UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
    UnweightedDegrees.undirectedDegrees(vertexRDD, edgeRDD).collect().toSet shouldBe expectedOutput
    UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel))).collect().toSet shouldBe expectedOutput
  }

  trait SingleUndirectedEdgeTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L), (2L, 1L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val allZeroDegrees: Map[Long, Long] = Map(1L -> 0L, 2L -> 0L)
    private val validDegrees: Map[Long, Long] = Map(1L -> 1L, 2L -> 1L)

    val expectedOutputValidLabel = gbVertexList.map(v => (v, validDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputAllZeroDegrees = gbVertexList.map(v => (v, allZeroDegrees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single undirected edge" should "have correct in-degree" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.inDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct in-degree for valid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct in-degree for invalid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have correct out-degree" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.outDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct out-degree for valid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct out-degree for invalid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have correct undirected degree" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.undirectedDegrees(vertexRDD, edgeRDD)
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct undirected degree for valid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputValidLabel
  }

  "single undirected edge" should "have correct undirected degree for invalid label" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have correct in-degree when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have correct out-degree when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  "single undirected edge" should "have correct undirected degree when restricted to empty set of edge labels" in new SingleUndirectedEdgeTest {
    val results = UnweightedDegrees.undirectedDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set()))
    results.collect().toSet shouldEqual expectedOutputAllZeroDegrees
  }

  trait SingleDirectedEdgeTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((1L, 2L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

    private val invalidDegrees: Map[Long, Long] = Map(1L -> 0L, 2L -> 0L)
    private val validInDegrees: Map[Long, Long] = Map(1L -> 0L, 2L -> 1L)
    private val validOutDegrees: Map[Long, Long] = Map(1L -> 1L, 2L -> 0L)

    val expectedOutputInDegreeValidLabel =
      gbVertexList.map(v => (v, validInDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputOutDegreeValidLabel =
      gbVertexList.map(v => (v, validOutDegrees(v.physicalId.asInstanceOf[Long]))).toSet
    val expectedOutputInvalidLabel = gbVertexList.map(v => (v, invalidDegrees(v.physicalId.asInstanceOf[Long]))).toSet
  }

  "single directed edge" should "have correct in-degree" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.inDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have correct in-degree for valid label" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputInDegreeValidLabel
  }

  "single directed edge" should "have correct in-degree for invalid label" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.inDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(invalidEdgeLabel)))
    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  "single directed edge" should "have correct out-degree" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.outDegrees(vertexRDD, edgeRDD)

    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have correct out-degree for valid label" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(validEdgeLabel)))

    results.collect().toSet shouldEqual expectedOutputOutDegreeValidLabel
  }

  "single directed edge" should "have correct out-degree for invalid label" in new SingleDirectedEdgeTest {
    val results = UnweightedDegrees.outDegreesByEdgeLabel(vertexRDD, edgeRDD, Some(Set(invalidEdgeLabel)))

    results.collect().toSet shouldEqual expectedOutputInvalidLabel
  }

  trait BadGraphTest {
    val invalidEdgeLabel = "likes"
    val validEdgeLabel = "REALLY likes (wink wink)"
    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"

    val vertexIdList: List[Long] = List(1, 2)
    val edgeList: List[(Long, Long)] = List((4.toLong, 2L), (2L, 3L))

    val gbVertexList = vertexIdList.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set()))

    val gbEdgeList =
      edgeList.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst),
            validEdgeLabel, Set.empty[Property])
      })

    val vertexRDD: RDD[GBVertex] = sparkContext.parallelize(gbVertexList, defaultParallelism)
    val edgeRDD: RDD[GBEdge] = sparkContext.parallelize(gbEdgeList, defaultParallelism)

  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing out degrees" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = UnweightedDegrees.outDegrees(vertexRDD, edgeRDD).collect()
    }
  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing in degrees" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = UnweightedDegrees.inDegrees(vertexRDD, edgeRDD).collect()
    }

  }

  "bad graph with mismatched edge and vertex RDDs" should "throw spark exception when computing undirected degrees" in new BadGraphTest {
    intercept[org.apache.spark.SparkException] {
      val results = UnweightedDegrees.undirectedDegrees(vertexRDD, edgeRDD).collect()
    }
  }
}
