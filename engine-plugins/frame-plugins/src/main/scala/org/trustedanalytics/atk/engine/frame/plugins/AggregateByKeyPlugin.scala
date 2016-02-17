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

package org.trustedanalytics.atk.engine.frame.plugins

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.{ PythonRddStorage, SparkFrame }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object AggregateByKeyJsonFormat {
  implicit val AggregateByKeyArgsFormat = jsonFormat5(AggregateByKeyArgs)
}

import AggregateByKeyJsonFormat._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
@PluginDoc(oneLine = "Combine by key current frame.",
  extended = """Combines data of frame based on key by evaluating a function for each row.""")
class AggregateByKeyPlugin extends SparkCommandPlugin[AggregateByKeyArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/aggregate_by_key"

  /* This plugin executes python udfs; by default sparkcommandplugins have this property as false */
  override def executesPythonUdf = true

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: AggregateByKeyArgs)(implicit invocation: Invocation) = 2

  /**
   * Computes the aggregation on reference frame by applying user defined function (UDF) on each row
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AggregateByKeyArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val newColumns = arguments.columnNames.zip(arguments.columnTypes.map(x => x: DataType))
    val columnList = newColumns.map { case (name, dataType) => Column(name, dataType) }
    val newSchema = new FrameSchema(columnList)
    val frameRDD = PythonRddStorage.aggregateMapWith(frame.rdd, arguments.aggregateByColumnKeys, arguments.udf, newSchema, sc)
    engine.frames.tryNewFrame(CreateEntityArgs(
      description = Some("Creats new frame by applying custom aggregation on current referencing frame"))) {
      newFrame => newFrame.save(frameRDD)
    }
  }
}
