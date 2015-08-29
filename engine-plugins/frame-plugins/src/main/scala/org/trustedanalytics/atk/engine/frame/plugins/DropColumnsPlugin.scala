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
import org.trustedanalytics.atk.domain.frame.DropColumnsArgs
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Remove columns from the frame.",
  extended = """The data from the columns is lost.

Notes
-----
It is not possible to delete all columns from a frame.
At least one column needs to remain.
If it is necessary to delete all columns, then delete the frame.""")
class DropColumnsPlugin extends SparkCommandPlugin[DropColumnsArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/drop_columns"

  /**
   * Remove columns from a frame.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: DropColumnsArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val schema = frame.schema
    schema.validateColumnsExist(arguments.columns)
    require(schema.columnNamesExcept(arguments.columns).nonEmpty,
      "Cannot delete all columns, please leave at least one column remaining")

    // run the operation
    val result = frame.rdd.selectColumns(schema.dropColumns(arguments.columns).columnNames)
    frame.save(result)
  }
}
