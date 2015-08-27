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

package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.{ DropDuplicatesArgs }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, ArgDoc }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, MiscFrameFunctions }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Remove duplicate rows, keeping only one row per uniqueness criteria match
 *
 */
@PluginDoc(oneLine = "Modify the current frame, removing duplicate rows.",
  extended = """Remove data rows which are the same as other rows.
The entire row can be checked for duplication, or the search for duplicates
can be limited to one or more columns.
This modifies the current frame.""")
class DropDuplicatesPlugin extends SparkCommandPlugin[DropDuplicatesArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/drop_duplicates"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DropDuplicatesArgs)(implicit invocation: Invocation) = 2

  /**
   * Remove duplicate rows, keeping only one row per uniqueness criteria match
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropDuplicatesArgs)(implicit invocation: Invocation): UnitReturn = {
    // validate arguments
    val frame: SparkFrame = arguments.frame
    val columnNames = arguments.unique_columns match {
      case Some(columns) => frame.schema.validateColumnsExist(columns.value).toList
      case None => frame.schema.columnNames
    }
    // run operation
    val duplicatesRemovedFrame: FrameRdd = frame.rdd.dropDuplicatesByColumn(columnNames)
    frame.save(duplicatesRemovedFrame)
  }
}
