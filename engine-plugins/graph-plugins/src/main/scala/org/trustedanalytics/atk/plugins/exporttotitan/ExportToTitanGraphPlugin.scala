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

package org.trustedanalytics.atk.plugins.exporttotitan

import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.StorageFormats
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph._
import org.trustedanalytics.atk.domain.schema.EdgeSchema
import org.trustedanalytics.atk.engine.graph.GraphBuilderConfigFactory
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.{ GraphBuilder, GraphBuilderConfig }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }
import org.trustedanalytics.atk.graphbuilder.parser.InputSchema
import org.trustedanalytics.atk.graphbuilder.schema.GraphSchema

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Plugin responsible for exporting a Seamless Graph to a Titan Graph.
 */
@PluginDoc(oneLine = "Convert current graph to TitanGraph.",
  extended = """Convert this Graph into a TitanGraph object.
This will be a new graph backed by Titan with all of the data found in this
graph.""",
  returns = "A new TitanGraph.")
class ExportToTitanGraphPlugin extends SparkCommandPlugin[ExportGraph, GraphEntity] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   *
   * The colon ":" is used to to indicate command destination base classes, default classes or classes of a
   * specific storage type:
   *
   * - graph:titan means command is loaded into class TitanGraph
   * - graph: means command is loaded into class Graph, our default type which will be the Parquet-backed graph
   * - graph would mean command is loaded into class BaseGraph, which applies to all graph classes
   * - frame: and means command is loaded in class Frame.  Example: "frame:/assign_sample"
   * - model:logistic_regression  means command is loaded into class LogisticRegressionModel
   */
  override def name: String = "graph:/export_to_titan"

  /**
   * Number of jobs needs to be known to give a single progress bar
   * @param arguments command arguments: used if a command can produce variable number of jobs
   * @return number of jobs in this command
   */
  override def numberOfJobs(arguments: ExportGraph)(implicit invocation: Invocation): Int = 5

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ExportGraph)(implicit invocation: Invocation): GraphEntity = {
    val graphs = engine.graphs
    val seamlessGraph: SeamlessGraphMeta = graphs.expectSeamless(arguments.graph.id)
    validateLabelNames(seamlessGraph.edgeFrames, seamlessGraph.edgeLabels)
    val titanGraph: GraphEntity = engine.graphs.createGraph(
      new GraphTemplate(arguments.newGraphName, StorageFormats.HBaseTitan))

    val graphSchema = SchemaConverter.convert(seamlessGraph.frameSchemas)

    loadTitanGraph(createGraphBuilderConfig(titanGraph.storage), graphSchema,
      graphs.loadGbVertices(sc, seamlessGraph.graphEntity),
      graphs.loadGbEdges(sc, seamlessGraph.graphEntity))

    graphs.expectGraph(titanGraph.toReference)
  }

  /**
   * load the vertices and edges into a titan graph
   * @param gbConfig configuration to use for constructing this graph
   * @param vertexRDD RDD of GBVertex objects found in seamless graph
   * @param edgeRDD  RDD of GBVertex objects found in a seamless graph
   */
  def loadTitanGraph(gbConfig: GraphBuilderConfig, graphSchema: GraphSchema, vertexRDD: RDD[GBVertex], edgeRDD: RDD[GBEdge]) {
    val graphBuilder = new GraphBuilder(gbConfig)
    graphBuilder.titanSchemaManager.writeSchema(graphSchema)
    graphBuilder.buildGraphWithSpark(vertexRDD, edgeRDD)
  }

  /**
   * Create GraphBuilderConfig object that corresponds to the required graphName
   * @param backendStorageName: Name of titan graph to write to.
   * @return
   */
  def createGraphBuilderConfig(backendStorageName: String): GraphBuilderConfig = {
    new GraphBuilderConfig(new InputSchema(List()),
      List(),
      List(),
      GraphBuilderConfigFactory.getTitanConfiguration(backendStorageName),
      inferSchema = false)
  }

  /**
   * Validate label names: Titan does not allow labels to be the same as property names.
   */
  def validateLabelNames(edgeFrames: List[FrameEntity], edgeLabels: List[String]) = {
    val invalidColumnNames = edgeFrames.flatMap(frame => frame.schema.columnNames.map(columnName => {
      if (edgeLabels.contains(columnName))
        s"Edge: ${frame.schema.asInstanceOf[EdgeSchema].label} Column: $columnName"
      else
        ""
    })).filter(s => !s.isEmpty)
    require(invalidColumnNames.isEmpty,
      s"Titan does not allow properties with the same key as an edge label. Please rename the following columns:\n\t${invalidColumnNames.mkString("\n\t")}")
  }
}
