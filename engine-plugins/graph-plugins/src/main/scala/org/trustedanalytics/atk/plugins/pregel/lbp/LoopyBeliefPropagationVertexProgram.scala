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

import org.apache.spark.graphx._
import org.trustedanalytics.atk.graphbuilder.elements.GBVertex
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.{ Initializers, VertexState }
import org.trustedanalytics.atk.graphbuilder.elements.Property

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
    val messageValues: List[Vector[Double]] = messages.map({ case (id, values) => values }).toList
    val productOfPriorAndMessages = VectorMath.overflowProtectedProduct(prior :: messageValues).get
    val posterior = VectorMath.l1Normalize(productOfPriorAndMessages)

    val oldPosterior = vertexState.posterior
    val delta = posterior.zip(oldPosterior).map({ case (x, y) => Math.abs(x - y) }).sum

    VertexState(vertexState.gbVertex, messages, prior, posterior, delta)
  }

}
