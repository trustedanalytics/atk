package org.trustedanalytics.atk.plugins.pregel.lbp

import org.apache.spark.graphx.{ EdgeTriplet, _ }
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.{ PregelAlgorithm, VertexState }

object LoopyBeliefPropagationMessage {
  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  def send(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

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

    val messagesNotFromDestination = messages - destination
    val messagesNotFromDestinationValues: List[Vector[Double]] =
      messagesNotFromDestination.map({ case (k, v) => v }).toList

    val reducedMessages = VectorMath.overflowProtectedProduct(prior :: messagesNotFromDestinationValues).get
    val statesUnPosteriors = stateRange.zip(reducedMessages)
    val unnormalizedMessage = stateRange.map(i => statesUnPosteriors.map({
      case (j, x: Double) =>
        x * Math.exp(PregelAlgorithm.edgePotential(i, j, edgeWeight))
    }).sum)

    val message = VectorMath.l1Normalize(unnormalizedMessage)

    Map(sender -> message)
  }

}
