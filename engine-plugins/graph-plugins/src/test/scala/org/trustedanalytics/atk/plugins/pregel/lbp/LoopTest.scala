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

package org.trustedanalytics.atk.plugins.pregel.lbp

import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.trustedanalytics.atk.plugins.pregel.core.{ TestInitializers, DefaultTestValues, PregelAlgorithm }
import org.trustedanalytics.atk.plugins.testutils.ApproximateVertexEquality
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * These test cases validate that belief propagation works correctly on (very simple) graphs that contain loops.
 */
class LoopTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LBPTest {
    val priorVector = Vector(0.5d, 0.5d)
    val args = TestInitializers.defaultPregelArgs()
  }

  "LBP Runner" should "work with a triangle with uniform probabilities" in new LBPTest {

    val vertexSet: Set[Long] = Set(1, 2, 3)
    val pdfValues: Map[Long, Vector[Double]] = Map(
      1.toLong -> priorVector,
      2.toLong -> priorVector,
      3.toLong -> priorVector)

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set(
      (1.toLong, 2.toLong),
      (1.toLong, 3.toLong),
      (2.toLong, 3.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
    )
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(DefaultTestValues.outputPropertyName), DefaultTestValues.floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "LBP Runner" should "work with a four-cycle with uniform probabilities" in new LBPTest {

    val vertexSet: Set[Long] = Set(1, 2, 3, 4)
    val pdfValues: Map[Long, Vector[Double]] = Map(
      1.toLong -> priorVector,
      2.toLong -> priorVector,
      3.toLong -> priorVector,
      4.toLong -> priorVector)

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set(
      (1.toLong, 2.toLong),
      (2.toLong, 3.toLong),
      (3.toLong, 4.toLong),
      (4.toLong, 1.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

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
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
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
