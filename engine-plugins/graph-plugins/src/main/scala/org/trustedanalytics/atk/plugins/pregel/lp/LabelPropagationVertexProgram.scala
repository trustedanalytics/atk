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

import org.apache.spark.graphx._
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.{ DefaultValues, VertexState }

object LabelPropagationVertexProgram {
  /**
   * Pregel required method to update the state of a vertex from the messages it has received.
   * @param id The id of the currently processed vertex.
   * @param vertexState The state of the currently processed vertex.
   * @param messages A map of the (neighbor, message-from-neighbor) pairs for the most recent round of message passing.
   * @return New state of the vertex.
   */
  def pregelVertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

    val messageValues: List[Vector[Double]] = messages.toList.map({ case (id, value) => value })
    if (messageValues.length <= 0) {
      // ignore spurious calls with empty message set
      // return the current state
      vertexState
    }
    else {
      //parameter validation
      val features = vertexState.stateSpaceSize
      val wasLabeled = vertexState.wasLabeled
      val alpha = vertexState.alpha
      val prior = vertexState.prior
      val posterior = vertexState.posterior
      val inWeight = vertexState.inWeight

      // Update posterior if the vertex was not processed
      val (calculatedPosterior: Vector[Double], updatedPrior: Vector[Double], delta: Double, updatedInWeight: Option[Double]) =
        if (wasLabeled == false) {
          val (calculatedInWeight, continueVertexProcessing) = inWeight match {
            case Some(value) => (value, true)
            case None => (calculateInWeight(messageValues), false)
          }
          if (continueVertexProcessing) {
            val weightedAvgPosterior = weightedAvg(features, calculatedInWeight, messageValues)
            val regularizedPosterior = applyRegularization(prior, weightedAvgPosterior, alpha)
            val normalizedPosterior = VectorMath.l1Normalize(regularizedPosterior)
            val delta = calculateDelta(normalizedPosterior, posterior)

            (normalizedPosterior, prior, delta, Some(calculatedInWeight))
          }
          else {
            // we got here after the initial message which is just the weights on links
            // only calculate the total weight on vertex & set delta to 1 for mor processing
            (posterior, prior, 1d, Some(calculatedInWeight))
          }
        }
        else {
          // return original values
          // do not relabel the nodes
          (posterior, prior, DefaultValues.deltaDefault, inWeight)
        }

      VertexState(vertexState.gbVertex,
        messages,
        updatedPrior,
        calculatedPosterior,
        delta,
        updatedInWeight,
        wasLabeled,
        alpha,
        features)
    }
  }

  private def sumAtIndex(msgValues: List[Vector[Double]], index: Int): Double = {
    var avg: Double = 0d
    for (i <- 0 until msgValues.length) {
      avg = avg + msgValues(i)(index)
    }

    avg
  }

  private def calculateInWeight(msgValues: List[Vector[Double]]): Double = {
    sumAtIndex(msgValues, 0)
  }

  private def applyRegularization(prior: Vector[Double], posterior: Vector[Double], alpha: Double): Vector[Double] = {
    (
      if (prior.length == posterior.length) {
        for {
          i <- 0 until posterior.length
        } yield (posterior(i) * (1 - alpha) + prior(i) * (alpha))
      }
      else {
        // this should never happen
        // consider an exception?
        posterior
      }
    ).toVector
  }

  private def calculateDelta(normalizedPosterior: Vector[Double], originalPosterior: Vector[Double]): Double = {
    normalizedPosterior.zip(originalPosterior).map(
      {
        case (finalP, originalP) => Math.abs(finalP - originalP)
      }
    ).sum
  }

  private def weightedAvg(features: Int, calculatedInWeight: Double, messageValues: List[Vector[Double]]): Vector[Double] = {
    (
      for {
        i <- 0 until features
      } yield (sumAtIndex(messageValues, i) / calculatedInWeight)
    ).toVector
  }

}
