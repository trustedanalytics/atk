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

import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.testutils.ApproximateVertexEquality
import org.apache.spark.rdd.RDD
import org.scalatest.{ Matchers, FlatSpec }
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * For graphs that are trees, belief propagation is known to converge to the exact solution with a number of iterations
 * bounded by the diameter of the graph.
 *
 * These tests validate that our LBP arrives at the correct answer for some small trees.
 */
class TreesTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

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

  "BP Runner" should "work properly on a five node tree with degree sequence 1, 1, 3, 2, 1" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5)

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set((1.toLong, 3.toLong), (2.toLong, 3.toLong), (3.toLong, 4.toLong),
      (4.toLong, 5.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val firstNodePriors = Vector(0.1d, 0.9d)
    val secondNodePriors = Vector(0.2d, 0.8d)
    val thirdNodePriors = Vector(0.3d, 0.7d)
    val fourthNodePriors = Vector(0.4d, 0.6d)
    val fifthNodePriors = Vector(0.5d, 0.5d)

    val priors: Map[Long, Vector[Double]] = Map(1.toLong -> firstNodePriors, 2.toLong -> secondNodePriors,
      3.toLong -> thirdNodePriors, 4.toLong -> fourthNodePriors, 5.toLong -> fifthNodePriors)

    // messages that converge after the first round - that is,  the constant ones coming from the leaves

    val potentialAt1 = 1.0d / (Math.E * Math.E)

    val message1to3 = Vector(firstNodePriors.head + firstNodePriors.last * potentialAt1,
      firstNodePriors.head * potentialAt1 + firstNodePriors.last)

    val message2to3 = Vector(secondNodePriors.head + secondNodePriors.last * potentialAt1,
      secondNodePriors.head * potentialAt1 + secondNodePriors.last)

    val message5to4 = Vector(fifthNodePriors.head + fifthNodePriors.last * potentialAt1,
      fifthNodePriors.head * potentialAt1 + fifthNodePriors.last)

    // messages the converge after the second round, after the leaves have reported

    val message3to4 = Vector(thirdNodePriors.head * message1to3.head * message2to3.head
      + potentialAt1 * thirdNodePriors.last * message1to3.last * message2to3.last,
      potentialAt1 * thirdNodePriors.head * message1to3.head * message2to3.head
        + thirdNodePriors.last * message1to3.last * message2to3.last
    )

    val message4to3 = Vector(fourthNodePriors.head * message5to4.head
      + potentialAt1 * fourthNodePriors.last * message5to4.last,
      potentialAt1 * fourthNodePriors.head * message5to4.head
        + fourthNodePriors.last * message5to4.last
    )

    // messages converging in the third round

    val message4to5 = Vector(fourthNodePriors.head * message3to4.head
      + potentialAt1 * fourthNodePriors.last * message3to4.last,
      potentialAt1 * fourthNodePriors.head * message3to4.head
        + fourthNodePriors.last * message3to4.last
    )

    val message3to1 = Vector(
      thirdNodePriors.head * message4to3.head * message2to3.head
        + potentialAt1 * thirdNodePriors.last * message4to3.last * message2to3.last,
      potentialAt1 * thirdNodePriors.head * message4to3.head * message2to3.head
        + thirdNodePriors.last * message4to3.last * message2to3.last
    )

    val message3to2 = Vector(
      thirdNodePriors.head * message4to3.head * message1to3.head
        + potentialAt1 * thirdNodePriors.last * message4to3.last * message1to3.last,
      potentialAt1 * thirdNodePriors.head * message4to3.head * message1to3.head
        + thirdNodePriors.last * message4to3.last * message1to3.last
    )

    // calculate expected posteriors

    val expectedPosterior1 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(firstNodePriors, message3to1).get)
    val expectedPosterior2 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(secondNodePriors, message3to2).get)
    val expectedPosterior3 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(List(thirdNodePriors, message1to3, message2to3, message4to3)).get)
    val expectedPosterior4 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(List(fourthNodePriors, message3to4, message5to4)).get)
    val expectedPosterior5 = VectorMath.l1Normalize(VectorMath.overflowProtectedProduct(fifthNodePriors, message4to5).get)

    val expectedPosteriors: Map[Long, Vector[Double]] = Map(1.toLong -> expectedPosterior1, 2.toLong -> expectedPosterior2,
      3.toLong -> expectedPosterior3, 4.toLong -> expectedPosterior4, 5.toLong -> expectedPosterior5)

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
