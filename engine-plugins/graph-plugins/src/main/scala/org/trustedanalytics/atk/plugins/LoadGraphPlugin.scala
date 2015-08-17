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

package org.trustedanalytics.atk.plugins

import org.trustedanalytics.atk.domain.graph.{ GraphEntity, LoadGraphArgs }
import org.trustedanalytics.atk.engine.frame.SparkFrameStorage
import org.trustedanalytics.atk.engine.graph.GraphBuilderConfigFactory
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.graphbuilder.driver.spark.titan.GraphBuilder
import org.apache.spark.rdd.RDD

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Loads graph data into a graph in the database.",
  extended = "The source is tabular data interpreted by user-specified rules.")
class LoadGraphPlugin extends SparkCommandPlugin[LoadGraphArgs, GraphEntity] {

  /**
   * The name of the command, e.g. graph/vertex_sample
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "graph:titan/load"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LoadGraphArgs)(implicit invocation: Invocation) = 3

  /**
   * Loads graph data into a graph in the database. The source is tabular data interpreted by user-specified rules.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LoadGraphArgs)(implicit invocation: Invocation): GraphEntity = {
    // dependencies (later to be replaced with dependency injection)
    val graphs = engine.graphs
    val frames = engine.frames.asInstanceOf[SparkFrameStorage]

    // validate arguments
    arguments.frameRules.foreach(frule => frames.expectFrame(frule.frame))
    val frameRules = arguments.frameRules
    // TODO graphbuilder only supports one input frame at present
    require(frameRules.size == 1, "only one frame rule per call is supported in this version")
    val theOnlySourceFrameID = frameRules.head.frame
    val frameEntity = frames.expectFrame(theOnlySourceFrameID)
    val graphEntity = graphs.expectGraph(arguments.graph)

    // setup graph builder
    val gbConfigFactory = new GraphBuilderConfigFactory(frameEntity.schema, arguments, graphEntity)
    val graphBuilder = new GraphBuilder(gbConfigFactory.graphConfig)

    // setup data in Spark
    val inputRowsRdd = frames.loadFrameData(sc, frameEntity)
    val inputRdd: RDD[Seq[_]] = inputRowsRdd.mapRows(x => x.toSeq)
    graphBuilder.build(inputRdd)

    graphEntity
  }

}
