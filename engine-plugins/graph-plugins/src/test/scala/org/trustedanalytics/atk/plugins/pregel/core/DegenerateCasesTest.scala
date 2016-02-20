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
package org.trustedanalytics.atk.plugins.pregel.core

import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.plugins.pregel.lbp.LoopyBeliefPropagationVertexProgram
import org.trustedanalytics.atk.plugins.testutils.ApproximateVertexEquality
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test makes sure that Pregel can correctly handle graphs with no edges and even graphs with
 * no vertices.
 */
class DegenerateCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait PregelTest {
    val args = TestInitializers.defaultPregelArgs()
  }

  "Pregel Runner" should "work properly with an empty graph" in new PregelTest {

    val vertexSet: Set[Long] = Initializers.defaultVertexSet()
    val pdfValues: Map[Long, Vector[Double]] = Initializers.defaultMsgSet()

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst),
            DefaultTestValues.edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(DefaultTestValues.vertexIdPropertyName, vid),
          Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
            Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      null,
      null,
      null
    )
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut
  }

  "Pregel Runner" should "work properly with a single node graph" in new PregelTest {

    val vertexSet: Set[Long] = Set(1)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.550d, 0.45d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x,
      Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst),
            DefaultTestValues.edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(DefaultTestValues.vertexIdPropertyName, vid),
          Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
            Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      TestInitializers.defaultMsgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      TestInitializers.defaultMsgSender
    )
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut
  }

  "Pregel Runner" should "work properly with a two node disconnected graph" in new PregelTest {

    val vertexSet: Set[Long] = Set(1, 2)
    val pdfValues: Map[Long, Vector[Double]] = Map(
      1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(0.1d, 0.9d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x,
      Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst),
            DefaultTestValues.edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(DefaultTestValues.vertexIdPropertyName, vid),
          Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
            Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      TestInitializers.defaultMsgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      TestInitializers.defaultMsgSender
    )
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(DefaultTestValues.outputPropertyName), DefaultTestValues.floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }

}
