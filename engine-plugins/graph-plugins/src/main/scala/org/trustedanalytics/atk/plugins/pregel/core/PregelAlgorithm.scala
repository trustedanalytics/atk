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
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, Property, GBVertex }
import org.trustedanalytics.atk.plugins.VectorMath
import org.apache.commons.lang3.StringUtils

/**
 * Arguments for the BeliefPropagationRunner
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperty Name of the property containing the priors.
 * @param maxIterations Maximum number of iterations to execute BP message passing.
 * @param stringOutput When true, the output is a comma-delimited string, when false (default) the output is a vector.
 * @param convergenceThreshold Optional Double. BP will terminate when average change in posterior beliefs between
 *                             supersteps is less than or equal to this threshold. Defaults to 0.
 * @param edgeWeightProperty Optional. Property containing edge weights.
 */
case class PregelArgs(posteriorProperty: String,
                      priorProperty: String,
                      maxIterations: Int,
                      stringOutput: Boolean,
                      convergenceThreshold: Double,
                      edgeWeightProperty: String,
                      stateSpaceSize: Int)
/**
 * Provides a method for running belief propagation on a graph. The result is a new graph with the belief-propagation
 * posterior beliefs placed in a new vertex property on each vertex.
 */
object PregelAlgorithm extends Serializable {

  val separators = DefaultValues.separatorDefault

  /**
   * Run belief propagation on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of belief propagation.
   * @return Vertex and edge list for the output graph and a logging string reporting on the execution of the belief
   *         propagation run.
   */
  def run(inVertices: RDD[GBVertex],
          inEdges: RDD[GBEdge],
          args: PregelArgs,
          wasLabeledPropertyName: Option[String] = None,
          alpha: Option[Float] = None)(initialMsgSender: (EdgeTriplet[VertexState, Double]) => Iterator[(VertexId, Map[Long, Vector[Double]])],
                                       vertexProgram: (VertexId, VertexState, Map[Long, Vector[Double]]) => VertexState,
                                       msgSender: (EdgeTriplet[VertexState, Double]) => Iterator[(VertexId, Map[Long, Vector[Double]])]): (RDD[GBVertex], RDD[GBEdge], String) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputPropertyName: String = args.priorProperty
    val maxIterations = args.maxIterations
    val convergenceThreshold = args.convergenceThreshold
    val stateSpaceSize = args.stateSpaceSize

    val firstVertexOption: Option[GBVertex] = try {
      Some(inVertices.first())
    }
    catch {
      case e: UnsupportedOperationException => None
    }

    if (firstVertexOption.isEmpty) {
      (inVertices, inEdges, "Attempt to run belief propagation on a graph with no vertices. No output.")
    }
    else {

      val firstVertex = firstVertexOption.get
      val firstPropertyOption = firstVertexOption.get.getProperty(inputPropertyName)

      if (firstPropertyOption.isEmpty) {
        throw new NotFoundException("Vertex Property ", inputPropertyName, vertexErrorInfo(firstVertex))
      }
      else {
        // convert to graphX vertices
        val graphXVertices: RDD[(Long, VertexState)] =
          inVertices.map(gbVertex => {
            val wasLabeled = getPropertyValue(gbVertex, wasLabeledPropertyName)
            (gbVertex.physicalId.asInstanceOf[Long],
              vertexStateFromVertex(gbVertex, inputPropertyName, stateSpaceSize, wasLabeled, alpha.getOrElse(0f)))
          })

        val graphXEdges = inEdges.map(edge => {
          edgeStateFromEdge(edge, args.edgeWeightProperty, DefaultValues.edgeWeightDefault)
        })
        val graph = Graph[VertexState, Double](graphXVertices, graphXEdges).partitionBy(PartitionStrategy.RandomVertexCut)

        val pregelWrapper = new PregelWrapper(maxIterations, convergenceThreshold)
        val (newGraph, log) = pregelWrapper.run(graph)(initialMsgSender, vertexProgram, msgSender)

        val outVertices = newGraph.vertices.map({
          case (vid, vertexState) =>
            vertexFromVertexState(vertexState, outputPropertyLabel, args.stringOutput)
        })

        (outVertices, inEdges, log)
      }
    }
  }

  /**
   * The edge potential function provides an estimate of how compatible the states are between two joined vertices.
   * This is the one inspired by the Boltzmann distribution.
   * @param state1 State of the first vertex.
   * @param state2 State of the second vertex.
   * @param weight Edge weight.
   * @return Compatibility estimate for the two states..
   */
  def edgePotential(state1: Int, state2: Int, weight: Double): Double = {

    val compatibilityFactor =
      if (DefaultValues.powerDefault == 0d) {
        if (state1 == state2)
          0d
        else
          1d
      }
      else {
        val delta = Math.abs(state1 - state2)
        Math.pow(delta, DefaultValues.powerDefault)
      }

    -1.0d * compatibilityFactor * weight * DefaultValues.smoothingDefault
  }

  /**
   * converts incoming edge to the form consumed by the belief propagation computation
   */
  private def edgeStateFromEdge(gbEdge: GBEdge,
                                edgeWeightPropertyName: String,
                                defaultEdgeWeight: Double): Edge[Double] = {

    val weight: Double = if (edgeWeightPropertyName.nonEmpty) {
      val property = gbEdge.getProperty(edgeWeightPropertyName)
      if (property.isEmpty) {
        throw new NotFoundException("Edge Property ", edgeWeightPropertyName, edgeErrorInfo(gbEdge))
      }
      else
        property match {
          case Some(kvPair) => DataTypes.toDouble(kvPair.value)
          case None => defaultEdgeWeight
        }
    }
    else {
      defaultEdgeWeight
    }

    new Edge[Double](gbEdge.tailPhysicalId.asInstanceOf[Long],
      gbEdge.headPhysicalId.asInstanceOf[Long],
      weight)
  }

  /**
   * Converts incoming vertex to the form consumed by the belief propagation computation
   */
  private def vertexStateFromVertex(gbVertex: GBVertex,
                                    inputPropertyName: String,
                                    stateSpaceSize: Int,
                                    wasLabeled: Boolean,
                                    alpha: Float): VertexState = {

    val priorProperty = gbVertex.getProperty(inputPropertyName)
    val prior: Vector[Double] = if (priorProperty.isEmpty) {
      throw new NotFoundException("Vertex Property ", inputPropertyName, vertexErrorInfo(gbVertex))
    }
    else {
      priorProperty.get.value match {
        case v: Vector[_] => v.asInstanceOf[Vector[Double]]
        case s: String => s.split(separators).filter(_.nonEmpty).map(_.toDouble).toVector
      }
    }

    val initialPrior = if (wasLabeled) {
      if (prior.length != stateSpaceSize) {
        throw new IllegalArgumentException("Length of prior does not match state space size" +
          System.lineSeparator() +
          vertexErrorInfo(gbVertex) +
          System.lineSeparator() +
          "Property name == " + inputPropertyName + "    Expected state space size " + stateSpaceSize)
      }
      else {
        prior
      }
    }
    else {
      VectorMath.l1Normalize(Array.fill[Double](stateSpaceSize)(DefaultValues.priorDefault).toVector)
    }
    val posterior = VectorMath.l1Normalize(initialPrior)

    VertexState(gbVertex,
      messages = Map(),
      prior = initialPrior,
      posterior = posterior,
      delta = DefaultValues.deltaDefault,
      wasLabeled = wasLabeled,
      alpha = alpha,
      stateSpaceSize = stateSpaceSize)

  }

  /**
   * Returns the value of a boolean property from gb vertex
   * @param gbVertex a graph vertex
   * @param propertyNameAsOption true if the property value is 1; false otherwise
   * @return
   */
  private def getPropertyValue(gbVertex: GBVertex,
                               propertyNameAsOption: Option[String]): Boolean = {
    val wasLabeled = 1
    val propertyName: String = propertyNameAsOption.getOrElse(StringUtils.EMPTY)
    val propertyValue: Int = if (propertyName != StringUtils.EMPTY) {
      gbVertex.getProperty(propertyName) match {
        case Some(kvPair) => DataTypes.toInt(kvPair.value)
        case None => wasLabeled
      }
    }
    else {
      wasLabeled
    }

    (propertyValue == wasLabeled)

  }

  /**
   * Converts vertex state into the common graph representation for output
   */
  private def vertexFromVertexState(vertexState: VertexState,
                                    outputPropertyLabel: String,
                                    beliefsAsStrings: Boolean) = {
    val oldGBVertex = vertexState.gbVertex

    val posteriorProperty: Property = if (beliefsAsStrings) {
      Property(outputPropertyLabel, vertexState.posterior.map(x => x.toString).mkString(", "))
    }
    else {
      Property(outputPropertyLabel, vertexState.posterior)
    }

    val properties = oldGBVertex.properties + posteriorProperty

    GBVertex(oldGBVertex.physicalId, oldGBVertex.gbId, properties)
  }

  /**
   * Creates error string for input vertex
   * @param gbVertex a gb vertex
   * @return a formatted string
   */
  private def vertexErrorInfo(gbVertex: GBVertex): String = {
    "Vertex ID == " + gbVertex.gbId.value.toString +
      "    Physical ID == " + gbVertex.physicalId
  }

  /**
   * Creates error string for input edge
   * @param gbEdge a gb edge
   * @return a formatted string
   */
  private def edgeErrorInfo(gbEdge: GBEdge): String = {
    "Edge ID == " + gbEdge.id + System.lineSeparator() +
      "Source Vertex == " + gbEdge.tailVertexGbId.value + System.lineSeparator() +
      "Destination Vertex == " + gbEdge.headVertexGbId.value
  }
}
