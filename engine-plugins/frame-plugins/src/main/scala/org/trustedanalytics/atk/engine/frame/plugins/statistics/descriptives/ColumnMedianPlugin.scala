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


package org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives

import org.trustedanalytics.atk.domain.frame.{ FrameReference, ColumnMedianArgs, ColumnMedianReturn }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate the median of the specified column.
 */
@PluginDoc(oneLine = "Calculate the (weighted) median of a column.",
  extended = """The median is the least value X in the range of the distribution so that
the cumulative weight of values strictly below X is strictly less than half
of the total weight and the cumulative weight of values up to and including X
is greater than or equal to one-half of the total weight.

All data elements of weight less than or equal to 0 are excluded from the
calculation, as are all data elements whose weight is NaN or infinite.
If a weight column is provided and no weights are finite numbers greater
than 0, None is returned.""",
  returns = """varies
    The median of the values.
    If a weight column is provided and no weights are finite numbers greater
    than 0, None is returned.
    The type of the median returned is the same as the contents of the data
    column, so a column of Longs will result in a Long median and a column of
    Floats will result in a Float median.""")
class ColumnMedianPlugin extends SparkCommandPlugin[ColumnMedianArgs, ColumnMedianReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/column_median"

  /**
   * Calculate the median of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for column median.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ColumnMedianArgs)(implicit invocation: Invocation): ColumnMedianReturn = {
    val frame: SparkFrame = arguments.frame
    val columnIndex = frame.schema.columnIndex(arguments.dataColumn)
    val valueDataType = frame.schema.columnDataType(arguments.dataColumn)

    // run the operation and return results
    val (weightsColumnIndexOption, weightsDataTypeOption) = if (arguments.weightsColumn.isEmpty) {
      (None, None)
    }
    else {
      val weightsColumnIndex = frame.schema.columnIndex(arguments.weightsColumn.get)
      (Some(weightsColumnIndex), Some(frame.schema.columnTuples(weightsColumnIndex)._2))
    }
    ColumnStatistics.columnMedian(columnIndex, valueDataType, weightsColumnIndexOption, weightsDataTypeOption, frame.rdd)
  }
}
