package org.trustedanalytics.atk.plugins.pregel.core

import org.apache.commons.lang3.StringUtils
import org.apache.spark.graphx._
import org.trustedanalytics.atk.graphbuilder.elements.{ GBVertex, Property }

object DefaultTestValues {
  val vertexIdPropertyName = "id"
  val srcIdPropertyName = "srcId"
  val dstIdPropertyName = "dstId"
  val edgeLabel = "label"
  val inputPropertyName = "input_property_name"
  val outputPropertyName = "output_property_name"
  val floatingPointEqualityThreshold: Double = 0.000000001d
  val maxIterations = 10
  val stringOutput = false
  val convergenceThreshold = 0d
  val stateSpaceSize = 2
}

object TestInitializers {

  /**
   * Pregel required method to update the state of a vertex from the messages it has received.
   * @param id The id of the currently processed vertex.
   * @param vertexState The state of the currently processed vertex.
   * @param messages A map of the (neighbor, message-from-neighbor) pairs for the most recent round of message passing.
   * @return New state of the vertex.
   */
  def defaultPregelVertexProgram(id: VertexId, vertexState: VertexState, messages: Map[VertexId, Vector[Double]]): VertexState = {

    val property = new Property("id", 1.toLong)
    val vect = Vector(0.50, 0.50)
    VertexState(new GBVertex(1.toLong, property, Set(property)), Initializers.defaultMsgSet(), vect, vect, 0.toLong)
  }

  /**
   * Pregel required method to send messages across an edge.
   * @param edgeTriplet Contains state of source, destination and edge.
   * @return Iterator over messages to send.
   */
  def defaultMsgSender(edgeTriplet: EdgeTriplet[VertexState, Double]): Iterator[(VertexId, Map[Long, Vector[Double]])] = {

    Iterator((edgeTriplet.dstId.toLong, Initializers.defaultMsgSet()))
  }

  /**
   * Creates a set of default pregel arguments
   * @return PregelArgs object
   */
  def defaultPregelArgs() = {
    PregelArgs(
      priorProperty = DefaultTestValues.inputPropertyName,
      edgeWeightProperty = StringUtils.EMPTY,
      maxIterations = DefaultTestValues.maxIterations,
      stringOutput = DefaultTestValues.stringOutput,
      convergenceThreshold = DefaultTestValues.convergenceThreshold,
      posteriorProperty = DefaultTestValues.outputPropertyName,
      stateSpaceSize = DefaultTestValues.stateSpaceSize)
  }
}
