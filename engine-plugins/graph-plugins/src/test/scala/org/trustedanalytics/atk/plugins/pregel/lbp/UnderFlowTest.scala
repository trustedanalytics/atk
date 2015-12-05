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
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex, Property }
import org.trustedanalytics.atk.plugins.pregel.core.{ PregelAlgorithm, PregelArgs }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * This test makes sure that we do not get underflow errors which cause some posteriors to become all zero vectors.
 *
 */
class UnderFlowTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait UFTest {

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

  "BP Runner" should "not have any all 0 posteriors" in new UFTest {

    // it's a 3x3 torus

    val vertexSet: Set[Long] = Set(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val edgeSet: Set[(Long, Long)] = Set((1, 2), (1, 4), (2, 3), (2, 5), (3, 1), (3, 6),
      (4, 5), (4, 7), (5, 6), (5, 8), (6, 4), (6, 9), (7, 8), (7, 1), (8, 9), (8, 2), (9, 7), (9, 3)).flatMap({ case (x, y) => Set((x.toLong, y.toLong), (y.toLong, x.toLong)) })

    val prior = Vector(0.9d, 0.1d)

    val gbVertexSet = vertexSet.map(x => GBVertex(x, Property(vertexIdPropertyName, x), Set(Property(inputPropertyName, prior))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst, Property(srcIdPropertyName, src), Property(dstIdPropertyName, dst), edgeLabel, Set.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(LoopyBeliefPropagationVertexProgram.loopyBeliefPropagation, LoopyBeliefPropagationMessage.send)

    val testVertices = verticesOut.collect().toSet
    val test = testVertices.forall(v => vectorStrictlyPositive(v.getProperty(propertyForLBPOutput).get.value.asInstanceOf[Vector[Double]]))

    test shouldBe true
  }

  private def vectorStrictlyPositive(v: Vector[Double]) = {
    v.forall(x => x >= 0d) && v.exists(x => x > 0d)
  }
}
