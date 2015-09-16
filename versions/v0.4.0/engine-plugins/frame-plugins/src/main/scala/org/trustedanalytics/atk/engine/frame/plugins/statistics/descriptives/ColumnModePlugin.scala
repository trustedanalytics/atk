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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives

import org.trustedanalytics.atk.domain.frame.{ ColumnModeArgs, ColumnModeReturn }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate modes of a column.
 */
@PluginDoc(oneLine = "Evaluate the weights assigned to rows.",
  extended = """Calculate the modes of a column.
A mode is a data element of maximum weight.
All data elements of weight less than or equal to 0 are excluded from the
calculation, as are all data elements whose weight is NaN or infinite.
If there are no data elements of finite weight greater than 0,
no mode is returned.

Because data distributions often have mutliple modes, it is possible for a
set of modes to be returned.
By default, only one is returned, but by setting the optional parameter
max_modes_returned, a larger number of modes can be returned.""",
  returns = """dict
    Dictionary containing summary statistics.
    The data returned is composed of multiple components:
mode : A mode is a data element of maximum net weight.
    A set of modes is returned.
    The empty set is returned when the sum of the weights is 0.
    If the number of modes is less than or equal to the parameter
    max_modes_returned, then all modes of the data are
    returned.
    If the number of modes is greater than the max_modes_returned
    parameter, only the first max_modes_returned many modes (per a
    canonical ordering) are returned.
weight_of_mode : Weight of a mode.
    If there are no data elements of finite weight greater than 0,
    the weight of the mode is 0.
    If no weights column is given, this is the number of appearances
    of each mode.
total_weight : Sum of all weights in the weight column.
    This is the row count if no weights are given.
    If no weights column is given, this is the number of rows in
    the table with non-zero weight.
mode_count : The number of distinct modes in the data.
    In the case that the data is very multimodal, this number may
    exceed max_modes_returned.

""")
class ColumnModePlugin extends SparkCommandPlugin[ColumnModeArgs, ColumnModeReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/column_mode"

  /**
   * Calculate modes of a column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ColumnModeArgs)(implicit invocation: Invocation): ColumnModeReturn = {
    val frame: SparkFrame = arguments.frame

    // run the operation and return results
    val dataColumn = frame.schema.column(arguments.dataColumn)
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columnDataType(arguments.weightsColumn.get)))
    }
    val modeCountOption = arguments.maxModesReturned

    ColumnStatistics.columnMode(dataColumn.index,
      dataColumn.dataType,
      weightsColumnIndexOption,
      weightsDataTypeOption,
      modeCountOption,
      frame.rdd)
  }
}
