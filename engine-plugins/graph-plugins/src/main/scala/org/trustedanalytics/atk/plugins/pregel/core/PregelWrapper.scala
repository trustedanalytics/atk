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

import org.apache.spark.graphx._
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.trustedanalytics.atk.pregel.{ AverageDeltaSuperStepStatusGenerator, BasicCountsInitialReport, DeltaProvider, Pregel }

/**
 * Internal state of a vertex during the progress of the belief propagation algorithm.
 * @param gbVertex The underlying vertex in the input graph. Used to propagate information out.
 * @param messages The messages that the vertex has received from its neighbors.
 * @param prior The prior probability distribution for this vertex.
 * @param posterior The current belief as informed by the latest round of message passing.
 * @param delta The difference between the new posterior belief and the last posterior belief.
 *              Used to gauge convergence.
 * @param inWeight The sum of in-message edge weights.
 * @param wasLabeled True if the vertex was labeled; false otherwise
 * @param alpha The posterior bias.
 */
case class VertexState(gbVertex: GBVertex,
                       messages: Map[VertexId, Vector[Double]],
                       prior: Vector[Double],
                       posterior: Vector[Double],
                       delta: Double,
                       inWeight: Option[Double] = None,
                       wasLabeled: Boolean = true,
                       alpha: Float = 0f,
                       stateSpaceSize: Int = 1) extends DeltaProvider with Serializable

/**
 * Provides a method to run belief propagation on a graph.
 * @param maxIterations Bound on the number of iterations.
 */
class PregelWrapper(val maxIterations: Int,
                    val convergenceThreshold: Double) extends Serializable {

  /**
   * Run belief propagation on a graph.
   *
   * @param graph GraphX graph to be analyzed.
   *
   * @return The graph with posterior probabilities updated by belief propagation, and a logging string
   *         reporting on the execution of the algorithm.
   */
  def run(graph: Graph[VertexState, Double])(initialMsgSender: (EdgeTriplet[VertexState, Double]) => Iterator[(VertexId, Map[Long, Vector[Double]])],
                                             vertexProgram: (VertexId, VertexState, Map[Long, Vector[Double]]) => VertexState,
                                             msgSender: (EdgeTriplet[VertexState, Double]) => Iterator[(VertexId, Map[Long, Vector[Double]])]): (Graph[VertexState, Double], String) = {

    // choose loggers
    val initialReporter = new BasicCountsInitialReport[VertexState, Double]
    val superStepReporter = new AverageDeltaSuperStepStatusGenerator[VertexState](convergenceThreshold)

    Pregel(graph,
      initialMsgSet,
      initialReporter,
      superStepReporter,
      maxIterations,
      EdgeDirection.Either)(initialMsgSender, vertexProgram, msgSender, msgCombiner)

  }

  /**
   * Pregel required method to combine messages coming into a vertex.
   *
   * @param m1 First message.
   * @param m2 Second message.
   * @return Combined message.
   */
  private def msgCombiner(m1: Map[Long, Vector[Double]],
                          m2: Map[Long, Vector[Double]]): Map[Long, Vector[Double]] = m1 ++ m2

  /**
   * Initial message for algorithm
   * @return an empty map
   */
  private def initialMsgSet(): Map[Long, Vector[Double]] = {
    Map().asInstanceOf[Map[Long, Vector[Double]]]
  }
}
