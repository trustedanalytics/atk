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

package org.trustedanalytics.atk.plugins.loopybeliefpropagation

import org.trustedanalytics.atk.plugins.testutils.ApproximateVertexEquality
import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * These tests validate loopy belief propagation on two node graphs.
 *
 * They provide easy to analyze examples for detecting many possible errors in
 * message calculation and in belief readout.
 */
class TwoNodeTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait BPTest {

    val vertexIdPropertyName = "id"
    val srcIdPropertyName = "srcId"
    val dstIdPropertyName = "dstId"
    val edgeLabel = "label"
    val inputPropertyName = "input_property_name"
    val propertyForLBPOutput = "LBP_VALUE"

    val floatingPointEqualityThreshold: Double = 0.000000001d

    val args = LoopyBeliefPropagationRunnerArgs(
      priorProperty = inputPropertyName,
      edgeWeightProperty = None,
      maxIterations = Some(10),
      stringOutput = None,
      convergenceThreshold = None,
      posteriorProperty = propertyForLBPOutput)

  }

  "BP Runner" should "work with two nodes of differing unit states" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d), 2.toLong -> Vector(0.0d, 1.0d))
    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d), 2.toLong -> Vector(0.0d, 1.0d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, expectedPosteriors.get(vid).get),
          Property(propertyForLBPOutput, priors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = LoopyBeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "BP Runner" should "work properly with a two node graph, uniform probabilities" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d))

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(0.5d, 0.5d),
      2.toLong -> Vector(0.5d, 0.5d))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, priors.get(vid).get),
          Property(propertyForLBPOutput, expectedPosteriors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = LoopyBeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "BP Runner" should "work properly with one node uniform, one node unit" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(0.5d, 0.5d))

    val potentialAt1 = 1.0d / (Math.E * Math.E)
    val oneOverPotentialAt1 = 1.0d / potentialAt1

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> Vector(1.0d, 0.0d),
      2.toLong -> Vector(oneOverPotentialAt1 / (oneOverPotentialAt1 + 1), 1 / (oneOverPotentialAt1 + 1)))

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, priors.get(vid).get),
          Property(propertyForLBPOutput, expectedPosteriors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = LoopyBeliefPropagationRunner.run(verticesIn, edgesIn, args)

    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val test = ApproximateVertexEquality.approximatelyEquals(testVertices,
      expectedVerticesOut,
      List(propertyForLBPOutput),
      floatingPointEqualityThreshold)

    test shouldBe true
    testEdges shouldBe expectedEdgesOut

  }

  "BP Runner" should "work properly with two nodes of differing non-uniform, non-unit priors" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val firstNodePriors = Vector(0.6d, 0.4d)
    val secondNodePriors = Vector(0.3d, 0.7d)

    val potentialAt1 = 1.0d / (Math.E * Math.E)

    val messageFirstToSecond = Vector(firstNodePriors.head + firstNodePriors.last * potentialAt1,
      (firstNodePriors.head * potentialAt1) + firstNodePriors.last)

    val messageSecondToFirst = Vector(secondNodePriors.head + secondNodePriors.last * potentialAt1,
      (secondNodePriors.head * potentialAt1) + secondNodePriors.last)

    val unnormalizedBeliefsFirstNode: Vector[Double] = firstNodePriors.zip(messageSecondToFirst).map({ case (p, m) => p * m })
    val unnormalizedBeliefsSecondNode: Vector[Double] = secondNodePriors.zip(messageFirstToSecond).map({ case (p, m) => p * m })

    val expectedFirstNodePosteriors = unnormalizedBeliefsFirstNode.map(x => x / unnormalizedBeliefsFirstNode.sum)
    val expectedSecondNodePosteriors = unnormalizedBeliefsSecondNode.map(x => x / unnormalizedBeliefsSecondNode.sum)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> firstNodePriors,
      2.toLong -> secondNodePriors)

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> expectedFirstNodePosteriors,
      2.toLong -> expectedSecondNodePosteriors)

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut =
      vertexSet.map(vid =>
        GBVertex(vid, Property(vertexIdPropertyName, vid), Set(Property(inputPropertyName, priors.get(vid).get),
          Property(propertyForLBPOutput, expectedPosteriors.get(vid).get))))

    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = LoopyBeliefPropagationRunner.run(verticesIn, edgesIn, args)

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
