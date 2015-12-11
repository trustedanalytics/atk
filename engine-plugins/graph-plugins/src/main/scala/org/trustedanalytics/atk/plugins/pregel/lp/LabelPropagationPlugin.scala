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

package org.trustedanalytics.atk.plugins.pregel.lp

import org.apache.commons.lang3.StringUtils
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.plugins.pregel.core._

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.plugins.pregel.core.JsonFormat._

/**
 * Launches loopy belief propagation.
 *
 * Pulls graph from underlying store, sends it off to the LBP runner, and then sends results
 * back to the underlying store.
 *
 * Right now it is using only Titan for graph storage. In time we will hopefully make this more flexible.
 *
 */
@PluginDoc(oneLine = "Classification on sparse data using Belief Propagation.",
  extended = """Belief propagation by the sum-product algorithm.
 This algorithm analyzes a graphical model with prior beliefs using sum product message passing.
 The priors are read from a property in the graph, the posteriors are written to another property in the graph.
 This is the GraphX-based implementation of belief propagation.

 See :ref:`Loopy Belief Propagation <python_api/frames/frame-/loopy_belief_propagation>`
 for a more in-depth discussion of |BP| and |LBP|.""",
  returns = "Progress report for belief propagation in the format of a multiple-line string.")
class LabelPropagationPlugin extends SparkCommandPlugin[PluginArgs, Return] {

  override def name: String = "graph:/label_propagation"

  //TODO remove when we move to the next version of spark
  override def kryoRegistrator: Option[String] = None

  override def numberOfJobs(arguments: PluginArgs)(implicit invocation: Invocation): Int = {
    9 + (arguments.maxIterations * 2)
  }

  override def execute(arguments: PluginArgs)(implicit invocation: Invocation): Return = {

    //parameter validation
    arguments.wasLabeledPropertyName.getOrElse(throw new RuntimeException("Was labeled property is needed for label propagation"))
    arguments.alpha.getOrElse(throw new RuntimeException("Alpha is needed for label propagation"))
    require(!StringUtils.isBlank(arguments.edgeWeightProperty), "Edge weight property is needed")

    val start = System.currentTimeMillis()

    // Get the graph
    val graph: SparkGraph = arguments.graph
    val (gbVertices, gbEdges) = graph.gbRdds

    val pregelArgs = PregelArgs(arguments.posteriorProperty,
      arguments.priorProperty,
      arguments.maxIterations,
      true,
      arguments.convergenceThreshold,
      arguments.edgeWeightProperty,
      arguments.stateSpaceSize)

    val (outVertices, outEdges, log) = PregelAlgorithm.run(gbVertices, gbEdges, pregelArgs, arguments.wasLabeledPropertyName, arguments.alpha)(
      LabelPropagationMessage.initialMsgSender,
      LabelPropagationVertexProgram.pregelVertexProgram,
      LabelPropagationMessage.msgSender
    )

    val frameRddMap = FrameRdd.toFrameRddMap(outVertices)
    val frameMap = frameRddMap.keys.map(label => {
      val result = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by connected components operation"))) {
        newOutputFrame: FrameEntity =>
          val frameRdd = frameRddMap(label)
          newOutputFrame.save(frameRdd)
      }
      (label, result)
    }).toMap
    val time = (System.currentTimeMillis() - start).toDouble / 1000.0

    Return(frameMap, time)
  }

}
