package org.trustedanalytics.atk.plugins.pregel.lbp

import org.apache.spark.graphx._
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.VertexState

object LoopyBeliefPropagationVertexProgram {
  /**
   * Pregel required method to update the state of a vertex from the messages it has received.
   * @param id The id of the currently processed vertex.
   * @param vertexState The state of the currently processed vertex.
   * @param messages A map of the (neighbor, message-from-neighbor) pairs for the most recent round of message passing.
   * @return New state of the vertex.
   */
  def pregelVertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

    val prior = vertexState.prior
    val messageValues: List[Vector[Double]] = messages.toList.map({ case (k, v) => v })
    val productOfPriorAndMessages = VectorMath.overflowProtectedProduct(prior :: messageValues).get
    val posterior = VectorMath.l1Normalize(productOfPriorAndMessages)

    val oldPosterior = vertexState.posterior
    val delta = posterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).sum

    VertexState(vertexState.gbVertex, messages, prior, posterior, delta)
  }

}
