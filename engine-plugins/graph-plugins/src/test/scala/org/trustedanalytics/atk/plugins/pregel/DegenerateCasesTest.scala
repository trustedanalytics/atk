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

package org.trustedanalytics.atk.plugins.pregel

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.plugins.pregel.core.{ PregelAlgorithm, PregelArgs }
import org.trustedanalytics.atk.plugins.testutils.ApproximateVertexEquality
import org.scalatest.Matchers
import org.scalatest.FlatSpec
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * These tests make sure that belief propagation can correctly handle graphs with no edges and even graphs with
 * no vertices. It sounds silly till the thing crashes on you.
 */
class DegenerateCasesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait BPTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = PregelArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = 10,
      stringOutput = false,
      convergenceThreshold = 0d,
      posteriorProperty = propertyForLBPOutput)

  }

  "BP Runner" should "work properly with an empty graph" in new BPTest {

    val vertexSet: Set[Long] = Set()
    val pdfValues: Map[Long, Vector[Double]] = Map()

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(srcIdPropertyName, src),
            Property(dstIdPropertyName, dst),
            edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(LoopyBeliefPropagationVertexProgram.loopyBeliefPropagation)
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut

  }

  "BP Runner" should "work properly with a single node graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.550d, 0.45d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(srcIdPropertyName, src),
            Property(dstIdPropertyName, dst),
            edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(LoopyBeliefPropagationVertexProgram.loopyBeliefPropagation)
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    testVertices shouldEqual expectedVerticesOut
    testEdges shouldBe expectedEdgesOut
  }

  "BP Runner" should "work properly with a two node disconnected graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)
    val pdfValues: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(0.1d, 0.9d))

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(srcIdPropertyName, src),
            Property(dstIdPropertyName, dst),
            edgeLabel, Set.empty[Property]
          )
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, pdfValues.get(vid).get),
          Property(propertyForLBPOutput, pdfValues.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)
    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(LoopyBeliefPropagationVertexProgram.loopyBeliefPropagation)
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }

}
