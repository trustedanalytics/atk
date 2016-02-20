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
package org.trustedanalytics.atk.plugins.pregel.lbp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.trustedanalytics.atk.plugins.pregel.core.{ TestInitializers, DefaultTestValues, PregelAlgorithm, PregelArgs }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * "Convergence threshold" in our system:
 * When the average change in posterior beliefs between supersteps falls below this threshold, terminate.
 *
 */
class ConvergenceThresholdTest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {

  trait LBPTest {

    val vertexSet: Set[Long] = Set(1, 2)

    val firstNodePriors = Vector(0.6d, 0.4d)
    val secondNodePriors = Vector(0.3d, 0.7d)

    val priors: Map[Long, Vector[Double]] = Map(
      1.toLong -> firstNodePriors,
      2.toLong -> secondNodePriors)

    //  directed edge list is made bidirectional with a flatmap
    val edgeSet: Set[(Long, Long)] = Set(
      (1.toLong,
        2.toLong)).flatMap({ case (x, y) => Set((x, y), (y, x)) })

    val gbVertexSet = vertexSet.map(x => GBVertex(x,
      Property(DefaultTestValues.vertexIdPropertyName, x),
      Set(Property(DefaultTestValues.inputPropertyName, priors.get(x).get))))

    val gbEdgeSet =
      edgeSet.map({
        case (src, dst) =>
          GBEdge(None, src, dst,
            Property(DefaultTestValues.srcIdPropertyName, src),
            Property(DefaultTestValues.dstIdPropertyName, dst), DefaultTestValues.edgeLabel, Set.empty[Property])
      })

    val verticesIn: RDD[GBVertex] = sparkContext.parallelize(gbVertexSet.toList)
    val edgesIn: RDD[GBEdge] = sparkContext.parallelize(gbEdgeSet.toList)

  }

  "LBP Runner" should "run for one iteration when convergence threshold is 1.0" in new LBPTest {

    val args = PregelArgs(
      priorProperty = DefaultTestValues.inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = DefaultTestValues.maxIterations,
      stringOutput = DefaultTestValues.stringOutput,
      convergenceThreshold = 1d,
      posteriorProperty = DefaultTestValues.outputPropertyName,
      stateSpaceSize = DefaultTestValues.stateSpaceSize)

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
    )

    log should include("Total number of iterations: 1")
  }

  "LBP Runner" should "run for one iteration when convergence threshold is 0.1" in new LBPTest {

    val args = PregelArgs(
      priorProperty = DefaultTestValues.inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = DefaultTestValues.maxIterations,
      stringOutput = DefaultTestValues.stringOutput,
      convergenceThreshold = 0.2d,
      posteriorProperty = DefaultTestValues.outputPropertyName,
      stateSpaceSize = DefaultTestValues.stateSpaceSize)

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
    )

    log should include("Total number of iterations: 1")
  }

  "LBP Runner" should "run for two iterations when  convergence threshold is 0" in new LBPTest {

    val args = TestInitializers.defaultPregelArgs()

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
    )

    log should include("Total number of iterations: 2")
  }

  // an example that slowly converges to an asymptote would make a better test when no threshold is given

  "LBP Runner" should "run for two iterations when no convergence threshold given" in new LBPTest {

    val args = TestInitializers.defaultPregelArgs()

    val (verticesOut, edgesOut, log) = PregelAlgorithm.run(verticesIn, edgesIn, args)(
      LoopyBeliefPropagationMessage.msgSender,
      LoopyBeliefPropagationVertexProgram.pregelVertexProgram,
      LoopyBeliefPropagationMessage.msgSender
    )

    log should include("Total number of iterations: 2")
  }
}
