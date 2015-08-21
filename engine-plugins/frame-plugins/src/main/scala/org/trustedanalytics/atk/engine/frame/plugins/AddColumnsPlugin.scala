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
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, PythonRddStorage }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
@PluginDoc(oneLine = "Add columns to current frame.",
  extended = """Assigns data to column based on evaluating a function for each row.

Notes
-----
    1)  The row |UDF| ('func') must return a value in the same format as
        specified by the schema.
        See :doc:`/ds_apir`.
    2)  Unicode in column names is not supported and will likely cause the
        drop_frames() method (and others) to fail!""")
class AddColumnsPlugin extends SparkCommandPlugin[AddColumnsArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/add_columns"

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: AddColumnsArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val newColumns = arguments.columnNames.zip(arguments.columnTypes.map(x => x: DataType))
    val columnList = newColumns.map { case (name, dataType) => Column(name, dataType) }
    val newSchema = new FrameSchema(columnList)

    val rdd = frame.rdd

    // Update the data
    // What we pass to PythonRddStorage is a stripped down version of FrameRdd if columnsAccessed is defined
    val addedColumnsRdd = arguments.columnsAccessed.isEmpty match {
      case true => PythonRddStorage.mapWith(rdd, arguments.udf, newSchema, sc)
      case false => PythonRddStorage.mapWith(rdd.selectColumns(arguments.columnsAccessed), arguments.udf, newSchema, sc)
    }
    frame.save(rdd.zipFrameRdd(addedColumnsRdd))
  }
}
