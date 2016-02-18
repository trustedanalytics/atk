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

import org.apache.spark.graphx.{ EdgeTriplet, _ }
import org.trustedanalytics.atk.plugins.VectorMath
import org.trustedanalytics.atk.plugins.pregel.core.{ VertexState }

object LabelPropagationMessage {
  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  def initialMsgSender(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    Iterator((edgeTriplet.dstId, initialMsg(edgeTriplet.srcId, edgeTriplet.srcAttr, edgeTriplet.attr)))
  }

  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  def msgSender(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    Iterator((edgeTriplet.dstId, calculateMsg(edgeTriplet.srcId, edgeTriplet.srcAttr, edgeTriplet.attr)))
  }

  /**
   * Calculates the initial message to be sent from one vertex to another.
   * @param sender ID of the vertex sending the message.
   * @param vertexState State of the sending vertex.
   * @param edgeWeight Weight of the edge joining the two vertices.
   * @return A map with one entry, sender -> messageToNeighbor
   */
  private def initialMsg(sender: VertexId,
                         vertexState: VertexState,
                         edgeWeight: Double): Map[VertexId, Vector[Double]] = {

    val newMessage = Array.fill[Double](vertexState.prior.length)(edgeWeight).toVector
    Map(sender -> newMessage)
  }

  /**
   * Calculates the message to be sent from one vertex to another.
   * @param sender ID of the vertex sending the message.
   * @param vertexState State of the sending vertex.
   * @param edgeWeight Weight of the edge joining the two vertices.
   * @return A map with one entry, sender -> messageToNeighbor
   */
  private def calculateMsg(sender: VertexId,
                           vertexState: VertexState,
                           edgeWeight: Double): Map[VertexId, Vector[Double]] = {

    val newMessage = vertexState.posterior.map { value => value * edgeWeight }
    Map(sender -> newMessage)
  }

}
