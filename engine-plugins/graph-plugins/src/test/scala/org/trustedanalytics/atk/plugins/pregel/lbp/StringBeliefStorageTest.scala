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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.driver.spark.elements.{ Property, GBEdge, GBVertex }
import org.trustedanalytics.atk.plugins.pregel.core.{ PregelAlgorithm, PregelArgs }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test verifies that the priors and posteriors can be read and stored as comma delimited lists in properties.
 *
 * The test will be deprecated when this functionality is deprecated.
 */
class StringBeliefStorageTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

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
      stringOutput = true,
      convergenceThreshold = 0d,
      posteriorProperty = propertyForLBPOutput,
      stateSpaceSize = 2)

  }

  "BeliefPropagationRunner with String Belief Storage" should "load and store properly with a two node disconnected graph" in new BPTest {

    val vertexSet: Set[Long] = Set(1, 2)
    val pdfValues: Map[Long, String] = Map(1.toLong -> "\t1.0    0.0  ", 2.toLong -> "0.1, 0.9d \t")

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Set()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, pdfValues.get(x).get))))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut = vertexSet.map(vid => GBVertex(vid, Property(vertexIdPropertyName, vid),
      Set(Property(inputPropertyName, pdfValues.get(vid).get), Property(propertyForLBPOutput, pdfValues.get(vid).get))))
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

    val testIdsToStrings: Map[Long, String] = testVertices.map(gbVertex =>
      (gbVertex.physicalId.asInstanceOf[Long],
        gbVertex.getProperty(propertyForLBPOutput).get.value.asInstanceOf[String])).toMap

    val testBelief1Option = testIdsToStrings.get(1)
    val testBelief2Option = testIdsToStrings.get(2)

    val test = if (testBelief1Option.isEmpty || testBelief2Option.isEmpty) {
      false
    }
    else {
      val testBelief1 = testBelief1Option.get.split(",").map(s => s.toDouble)
      val testBelief2 = testBelief2Option.get.split(",").map(s => s.toDouble)

      (Math.abs(testBelief1.apply(0) - 1.0d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief1.apply(1) - 0.0d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(0) - 0.1d) < floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(1) - 0.9d) < floatingPointEqualityThreshold)
    }

    test shouldBe true
    testEdges shouldBe expectedEdgesOut
  }
}
