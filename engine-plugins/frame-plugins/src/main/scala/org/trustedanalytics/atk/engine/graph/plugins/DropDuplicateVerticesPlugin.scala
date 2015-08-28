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

package org.trustedanalytics.atk.engine.graph.plugins

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.engine.graph.{ SparkVertexFrame, VertexFrame }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.trustedanalytics.atk.domain.frame.DropDuplicatesArgs
import org.apache.spark.atk.graph.VertexFrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.frame.{ SparkFrameStorage, MiscFrameFunctions }
import org.trustedanalytics.atk.domain.graph.SeamlessGraphMeta
import org.trustedanalytics.atk.domain.schema.{ GraphSchema, VertexSchema }

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Remove duplicate vertex rows.",
  extended = """Remove duplicate vertex rows, keeping only one vertex row per uniqueness
criteria match.
Edges that were connected to removed vertices are also automatically dropped.""")
class DropDuplicateVerticesPlugin extends SparkCommandPlugin[DropDuplicatesArgs, UnitReturn] {

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
  override def name: String = "frame:vertex/drop_duplicates"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 4

  /**
   * Plugins must implement this method to do the work requested by the user.
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments the arguments supplied by the caller
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): UnitReturn = {
    val vertexFrame: SparkVertexFrame = arguments.frame
    val seamlessGraph = vertexFrame.graphMeta
    val schema = vertexFrame.schema
    val columnNames = arguments.unique_columns match {
      case Some(columns) => schema.validateColumnsExist(columns.value).toList
      case None =>
        // _vid is always unique so don't include it
        schema.columnNames.dropWhile(s => s == GraphSchema.vidProperty)
    }
    val duplicatesRemovedFrame: VertexFrameRdd = vertexFrame.rdd.dropDuplicatesByColumn(columnNames)

    val label = schema.asInstanceOf[VertexSchema].label
    FilterVerticesFunctions.removeDanglingEdges(label, engine.frames, seamlessGraph, sc, duplicatesRemovedFrame)

    vertexFrame.save(duplicatesRemovedFrame)

  }
}
