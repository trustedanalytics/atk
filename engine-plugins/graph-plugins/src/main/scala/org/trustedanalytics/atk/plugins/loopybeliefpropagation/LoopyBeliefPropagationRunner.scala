/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.plugins.loopybeliefpropagation

import org.trustedanalytics.atk.plugins.VectorMath
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.graphbuilder.elements.{ Property, GBVertex, GBEdge }
import org.apache.spark.graphx.{ PartitionStrategy, Graph, Edge }
import org.trustedanalytics.atk._

/**
 * Arguments for the BeliefPropagationRunner
 * @param posteriorProperty Name of the property to which the posteriors will be written.
 * @param priorProperty Name of the property containing the priors.
 * @param maxIterations Maximum number of iteratiosn to execute BP message passing.
 * @param stringOutput When true, the output is a comma-delimited string, when false (default) the output is a vector.
 * @param convergenceThreshold Optional Double. BP will terminate when average change in posterior beliefs between
 *                             supersteps is less than or equal to this threshold. Defaults to 0.
 * @param edgeWeightProperty Optional. Property containing edge weights.
 */
case class LoopyBeliefPropagationRunnerArgs(posteriorProperty: String,
                                            priorProperty: String,
                                            maxIterations: Option[Int],
                                            stringOutput: Option[Boolean],
                                            convergenceThreshold: Option[Double],
                                            edgeWeightProperty: Option[String])
/**
 * Provides a method for running belief propagation on a graph. The result is a new graph with the belief-propagation
 * posterior beliefs placed in a new vertex property on each vertex.
 */
object LoopyBeliefPropagationRunner extends Serializable {

  val separators: Array[Char] = Array(' ', ',', '\t')

  /**
   * Run belief propagation on a graph.
   * @param inVertices Vertices of the incoming graph.
   * @param inEdges Edges of the incoming graph.
   * @param args Parameters controlling the execution of belief propagation.
   * @return Vertex and edge list for the output graph and a logging string reporting on the execution of the belief
   *         propagation run.
   */
  def run(inVertices: RDD[GBVertex], inEdges: RDD[GBEdge], args: LoopyBeliefPropagationRunnerArgs): (RDD[GBVertex], RDD[GBEdge], String) = {

    val outputPropertyLabel = args.posteriorProperty
    val inputPropertyName: String = args.priorProperty
    val maxIterations: Int = args.maxIterations.getOrElse(LoopyBeliefPropagationDefaults.maxIterationsDefault)
    val beliefsAsStrings = args.stringOutput.getOrElse(LoopyBeliefPropagationDefaults.stringOutputDefault)
    val convergenceThreshold = args.convergenceThreshold.getOrElse(LoopyBeliefPropagationDefaults.convergenceThreshold)

    val firstVertexOption: Option[GBVertex] = try {
      Some(inVertices.first())
    }
    catch {
      case e: UnsupportedOperationException => None
    }

    if (firstVertexOption.isEmpty) {
      (inVertices, inEdges, "Attempt to run belief propagation on a vertex free graph. No output.")
    }
    else {

      val firstVertex = firstVertexOption.get
      val firstPropertyOption = firstVertexOption.get.getProperty(inputPropertyName)

      if (firstPropertyOption.isEmpty) {
        throw new NotFoundException("Vertex Property", inputPropertyName, vertexErrorInfo(firstVertex))
      }
      else {
        val firstPrior = firstPropertyOption.get.value

        val stateSpaceSize: Int = firstPrior match {
          case v: Vector[_] => v.length
          case s: String => s.split(separators).filter(_.nonEmpty).map(_.toDouble).toVector.length
        }

        val defaultEdgeWeight = LoopyBeliefPropagationDefaults.edgeWeightDefault
        val power = LoopyBeliefPropagationDefaults.powerDefault
        val smoothing = LoopyBeliefPropagationDefaults.smoothingDefault

        // convert to graphX vertices
        val graphXVertices: RDD[(Long, VertexState)] =
          inVertices.map(gbVertex => (gbVertex.physicalId.asInstanceOf[Long], bpVertexStateFromVertex(gbVertex, inputPropertyName, stateSpaceSize)))

        val graphXEdges = inEdges.map(edge => bpEdgeStateFromEdge(edge, args.edgeWeightProperty, defaultEdgeWeight))

        val graph = Graph[VertexState, Double](graphXVertices, graphXEdges)
          .partitionBy(PartitionStrategy.RandomVertexCut)

        val graphXLBPRunner = new PregelBeliefPropagation(maxIterations, power, smoothing, convergenceThreshold)
        val (newGraph, log) = graphXLBPRunner.run(graph)

        val outVertices = newGraph.vertices.map({
          case (vid, vertexState) =>
            vertexFromBPVertexState(vertexState, outputPropertyLabel, beliefsAsStrings)
        })

        (outVertices, inEdges, log)
      }
    }
  }

  /**
   * converts incoming edge to the form consumed by the belief propagation computation
   */
  private def bpEdgeStateFromEdge(gbEdge: GBEdge, edgeWeightPropertyNameOption: Option[String], defaultEdgeWeight: Double) = {

    val weight: Double = if (edgeWeightPropertyNameOption.nonEmpty) {

      val edgeWeightPropertyName = edgeWeightPropertyNameOption.get

      val property = gbEdge.getProperty(edgeWeightPropertyName)

      if (property.isEmpty) {
        throw new NotFoundException("Edge Property", edgeWeightPropertyName, edgeErrorInfo(gbEdge))
      }
      else {
        gbEdge.getProperty(edgeWeightPropertyNameOption.get).get.asInstanceOf[Double]
      }
    }
    else {
      defaultEdgeWeight
    }
    val srcId = gbEdge.tailPhysicalId.asInstanceOf[Long]
    val destId = gbEdge.headPhysicalId.asInstanceOf[Long]

    new Edge[Double](srcId, destId, weight)
  }

  /**
   * Converts incoming vertex to the form consumed by the belief propagation computation
   */
  private def bpVertexStateFromVertex(gbVertex: GBVertex,
                                      inputPropertyName: String, stateSpaceSize: Int): VertexState = {

    val property = gbVertex.getProperty(inputPropertyName)

    val prior: Vector[Double] = if (property.isEmpty) {
      throw new NotFoundException("Vertex Property", inputPropertyName, vertexErrorInfo(gbVertex))
    }
    else {
      property.get.value match {
        case v: Vector[_] => v.asInstanceOf[Vector[Double]]
        case s: String => s.split(separators).filter(_.nonEmpty).map(_.toDouble).toVector
      }
    }

    if (prior.length != stateSpaceSize) {
      throw new IllegalArgumentException("Length of prior does not match state space size\n" +
        vertexErrorInfo(gbVertex) + "\n" +
        "Property name == " + inputPropertyName + "    Expected state space size " + stateSpaceSize)
    }
    val posterior = VectorMath.l1Normalize(prior)

    VertexState(gbVertex, messages = Map(), prior, posterior, delta = 0)

  }

  /**
   * converts vertex in belief propagation output into the common graph representation for output
   */
  private def vertexFromBPVertexState(vertexState: VertexState, outputPropertyLabel: String, beliefsAsStrings: Boolean) = {
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
    "Vertex ID ==" + gbVertex.gbId.value.toString +
      "    Physical ID == " + gbVertex.physicalId
  }

  /**
   * Creates error string for input edge
   * @param gbEdge a gb edge
   * @return a formatted string
   */
  private def edgeErrorInfo(gbEdge: GBEdge): String = {
    "Edge ID == " + gbEdge.id + "\n" +
      "Source Vertex == " + gbEdge.tailVertexGbId.value + "\n" +
      "Destination Vertex == " + gbEdge.headVertexGbId.value
  }
}
