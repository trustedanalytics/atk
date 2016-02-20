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

import org.apache.spark.graphx.{ EdgeTriplet, _ }
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.{ Initializers, PregelAlgorithm, VertexState }

object LoopyBeliefPropagationMessage {
  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  def msgSender(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    Iterator((edgeTriplet.dstId, calculate(edgeTriplet.srcId, edgeTriplet.dstId, edgeTriplet.srcAttr, edgeTriplet.attr)))
  }

  /**
   * Calculates the message to be sent from one vertex to another.
   * @param sender ID of he vertex sending the message.
   * @param destination ID of the vertex to receive the message.
   * @param vertexState State of the sending vertex.
   * @param edgeWeight Weight of the edge joining the two vertices.
   * @return A map with one entry, sender -> messageToNeighbor
   */
  private def calculate(sender: VertexId,
                        destination: VertexId,
                        vertexState: VertexState,
                        edgeWeight: Double): Map[VertexId, Vector[Double]] = {

    val prior = vertexState.prior
    val messages = vertexState.messages

    val nStates = prior.length
    val stateRange = (0 to nStates - 1).toVector

    val values: List[Vector[Double]] = (messages - destination).map({ case (id, values) => values }).toList
    val reducedMessages = VectorMath.overflowProtectedProduct(prior :: values).get
    val statesUnPosteriors = stateRange.zip(reducedMessages)
    val unnormalizedMessage = stateRange.map(i => statesUnPosteriors.map({
      case (j, x: Double) =>
        x * Math.exp(PregelAlgorithm.edgePotential(i, j, edgeWeight))
    }).sum)

    val message = VectorMath.l1Normalize(unnormalizedMessage)

    Map(sender -> message)
  }

}
