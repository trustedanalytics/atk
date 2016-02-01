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

package org.trustedanalytics.atk.plugins.pregel.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test verifies that the priors and posteriors can be read and stored as comma delimited lists in properties.
 *
 * The test will be deprecated when this functionality is deprecated.
 */
class StringBeliefStorageTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait PregelTest {

    val args = PregelArgs(
      priorProperty = DefaultTestValues.inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = DefaultTestValues.maxIterations,
      stringOutput = true,
      convergenceThreshold = DefaultTestValues.convergenceThreshold,
      posteriorProperty = DefaultTestValues.outputPropertyName,
      stateSpaceSize = DefaultTestValues.stateSpaceSize)

  }

  "BeliefPropagationRunner with String Belief Storage" should "load and store properly with a two node disconnected graph" in new PregelTest {

    val vertexSet: Set[Long] = Set(1.toLong, 2.toLong)
    val pdfValues: Map[Long, String] = Map(
      1.toLong -> "\t1.0    0.0  ",
      2.toLong -> "0.1, 0.9d \t")

    //  directed edge list is made bidirectional with a flatmap

    val edgeSet: Set[(Long, Long)] = Initializers.defaultEdgeSet()
    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(x).get))))
    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst), DefaultTestValues.edgeLabel, Set.empty[Property])
      })

    val expectedVerticesOut = vertexSet.map(vid => GBVertex(vid,
      Property(DefaultTestValues.vertexIdPropertyName, vid),
      Set(Property(DefaultTestValues.inputPropertyName, pdfValues.get(vid).get),
        Property(DefaultTestValues.outputPropertyName, pdfValues.get(vid).get))))
    val expectedEdgesOut = gbEdgeSet // no expected changes to the edge set
    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      TestInitializers.defaultMsgSender,
      TestInitializers.defaultPregelVertexProgram,
      TestInitializers.defaultMsgSender
    )
    val testVertices = verticesOut.collect().toSet
    val testEdges = edgesOut.collect().toSet

    val testIdsToStrings: Map[Long, String] = testVertices.map(gbVertex =>
      (gbVertex.physicalId.asInstanceOf[Long],
        gbVertex.getProperty(DefaultTestValues.outputPropertyName).get.value.asInstanceOf[String])).toMap

    val testBelief1Option = testIdsToStrings.get(1)
    val testBelief2Option = testIdsToStrings.get(2)

    val test = if (testBelief1Option.isEmpty || testBelief2Option.isEmpty) {
      false
    }
    else {
      val testBelief1 = testBelief1Option.get.split(",").map(s => s.toDouble)
      val testBelief2 = testBelief2Option.get.split(",").map(s => s.toDouble)

      (Math.abs(testBelief1.apply(0) - 1.0d) < DefaultTestValues.floatingPointEqualityThreshold) &&
        (Math.abs(testBelief1.apply(1) - 0.0d) < DefaultTestValues.floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(0) - 0.1d) < DefaultTestValues.floatingPointEqualityThreshold) &&
        (Math.abs(testBelief2.apply(1) - 0.9d) < DefaultTestValues.floatingPointEqualityThreshold)
    }

    test shouldBe false
    testEdges shouldBe expectedEdgesOut
  }
}
