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
import org.trustedanalytics.atk.pregel.{ AverageDeltaSuperStepStatusGenerator, BasicCountsInitialReport, Pregel, DeltaProvider }
import org.apache.spark.graphx._
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex

/**
 * Internal state of a vertex during the progress of the belief propagation algorithm.
 * @param gbVertex The underlying vertex in the input graph. Used to propagate information out.
 * @param messages The messages that the vertex has received from its neighbors.
 * @param prior The prior probability distribution for this vertex.
 * @param posterior The current belief as informed by the latest round of message passing.
 * @param delta The difference between the new posterior belief and the last posterior belief.
 *              Used to gauge convergence.
 */
case class VertexState(gbVertex: GBVertex,
                       messages: Map[VertexId, Vector[Double]],
                       prior: Vector[Double],
                       posterior: Vector[Double],
                       delta: Double) extends DeltaProvider with Serializable

/**
 * Provides a method to run belief propagation on a graph.
 * @param maxIterations Bound on the number of iterations.
 * @param power Exponent used in the potential function.
 * @param smoothing Smoothing parameter used in the potential function
 */
class PregelBeliefPropagation(val maxIterations: Int,
                              val power: Double,
                              val smoothing: Double,
                              val convergenceThreshold: Double) extends Serializable {

  /**
   * Run belief propagation on a graph.
   *
   * @param graph GraphX graph to be analyzed.
   *
   * @return The graph with posterior probabilities updated by belief propagation, and a logging string
   *         reporting on the execution of the algorithm.
   */
  def run(graph: Graph[VertexState, Double]): (Graph[VertexState, Double], String) = {

    // choose loggers
    val initialReporter = new BasicCountsInitialReport[VertexState, Double]
    val superStepReporter = new AverageDeltaSuperStepStatusGenerator[VertexState](convergenceThreshold)

    // call  Pregel
    org.trustedanalytics.atk.pregel.Pregel(graph,
      Map().asInstanceOf[Map[Long, Vector[Double]]],
      initialReporter,
      superStepReporter,
      maxIterations = maxIterations,
      activeDirection = EdgeDirection.Either)(vertexProgram, sendMessage, mergeMsg)

  }

  /**
   * Pregel required method to update the state of a vertex from the messages it has received.
   * @param id The id of the currently processed vertex.
   * @param vertexState The state of the currently processed vertex.
   * @param messages A map of the (neighbor, message-from-neighbor) pairs for the most recent round of message passing.
   * @return New state of the vertex.
   */
  private def vertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

    val prior = vertexState.prior

    val oldPosterior = vertexState.posterior

    val messageValues: List[Vector[Double]] = messages.toList.map({ case (k, v) => v })

    val productOfPriorAndMessages: Vector[Double] = VectorMath.overflowProtectedProduct(prior :: messageValues).get

    val posterior = VectorMath.l1Normalize(productOfPriorAndMessages)

    val delta = posterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).sum

    VertexState(vertexState.gbVertex, messages, prior, posterior, delta)
  }

  /**
   * The edge potential function provides an estimate of how compatible the states are between two joined vertices.
   * This is the one inspired by the Boltzmann distribution.
   * @param state1 State of the first vertex.
   * @param state2 State of the second vertex.
   * @param weight Edge weight.
   * @return Compatibility estimate for the two states..
   */
  private def edgePotential(state1: Int, state2: Int, weight: Double) = {

    val compatibilityFactor =
      if (power == 0d) {
        if (state1 == state2)
          0d
        else
          1d
      }
      else {
        val delta = Math.abs(state1 - state2)
        Math.pow(delta, power)
      }

    -1.0d * compatibilityFactor * weight * smoothing
  }

  /**
   * Calculates the message to be sent from one vertex to another.
   * @param sender ID of he vertex sending the message.
   * @param destination ID of the vertex to receive the message.
   * @param vertexState State of the sending vertex.
   * @param edgeWeight Weight of the edge joining the two vertices.
   * @return A map with one entry, sender -> messageToNeighbor
   */
  private def calculateMessage(sender: VertexId, destination: VertexId, vertexState: VertexState, edgeWeight: Double): Map[VertexId, Vector[Double]] = {

    val prior = vertexState.prior
    val messages = vertexState.messages

    val nStates = prior.length
    val stateRange = (0 to nStates - 1).toVector

    val messagesNotFromDestination = messages - destination
    val messagesNotFromDestinationValues: List[Vector[Double]] =
      messagesNotFromDestination.map({ case (k, v) => v }).toList

    val reducedMessages = VectorMath.overflowProtectedProduct(prior :: messagesNotFromDestinationValues).get

    val statesUNPosteriors = stateRange.zip(reducedMessages)

    val unnormalizedMessage = stateRange.map(i => statesUNPosteriors.map({
      case (j, x: Double) =>
        x * Math.exp(edgePotential(i, j, edgeWeight))
    }).sum)

    val message = VectorMath.l1Normalize(unnormalizedMessage)

    Map(sender -> message)
  }

  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  private def sendMessage(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    val vertexState = edgeTriplet.srcAttr

    Iterator((edgeTriplet.dstId, calculateMessage(edgeTriplet.srcId, edgeTriplet.dstId, vertexState, edgeTriplet.attr)))
  }

  /**
   * Pregel required method to combine messages coming into a vertex.
   *
   * @param m1 First message.
   * @param m2 Second message.
   * @return Combined message.
   */
  private def mergeMsg(m1: Map[Long, Vector[Double]], m2: Map[Long, Vector[Double]]): Map[Long, Vector[Double]] = m1 ++ m2
}
