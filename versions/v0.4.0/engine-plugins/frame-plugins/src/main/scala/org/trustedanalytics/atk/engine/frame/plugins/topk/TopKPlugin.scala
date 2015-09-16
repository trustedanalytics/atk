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

package org.trustedanalytics.atk.engine.frame.plugins.topk

import org.trustedanalytics.atk.domain.frame.{ FrameReference, TopKArgs, FrameEntity }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, Column, DataTypes }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate the top (or bottom) K distinct values by count for specified data column.
 *
 */
@PluginDoc(oneLine = "Most or least frequent column values.",
  extended = """Calculate the top (or bottom) K distinct values by count of a column.
The column can be weighted.
All data elements of weight <= 0 are excluded from the calculation, as are
all data elements whose weight is NaN or infinite.
If there are no data elements of finite weight > 0, then topK is empty.""",
  returns = "An object with access to the frame of data.")
class TopKPlugin extends SparkCommandPlugin[TopKArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/top_k"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: TopKArgs)(implicit invocation: Invocation) = 3

  /**
   * Calculate the top (or bottom) K distinct values by count for specified data column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TopKArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val columnIndex = frame.schema.columnIndex(arguments.columnName)

    // run the operation
    val valueDataType = frame.schema.columnDataType(arguments.columnName)
    val (weightsColumnIndexOption, weightsDataTypeOption) = getColumnIndexAndType(frame, arguments.weightsColumn)
    val useBottomK = arguments.k < 0
    val topRdd = TopKRddFunctions.topK(frame.rdd, columnIndex, Math.abs(arguments.k), useBottomK,
      weightsColumnIndexOption, weightsDataTypeOption)

    val newSchema = FrameSchema(List(
      Column(arguments.columnName, valueDataType),
      Column("count", DataTypes.float64)
    ))

    // save results
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by top k command"))) { newFrame =>
      newFrame.save(new FrameRdd(newSchema, topRdd))
    }
  }

  // TODO: replace getColumnIndexAndType() with methods on Schema

  /**
   * Get column index and data type of a column in a data frame.
   *
   * @param frame Data frame
   * @param columnName Column name
   * @return Option with the column index and data type
   */
  @deprecated("use methods on Schema instead")
  private def getColumnIndexAndType(frame: FrameEntity, columnName: Option[String]): (Option[Int], Option[DataType]) = {

    val (columnIndexOption, dataTypeOption) = columnName match {
      case Some(columnIndex) =>
        val weightsColumnIndex = frame.schema.columnIndex(columnIndex)
        (Some(weightsColumnIndex), Some(frame.schema.column(weightsColumnIndex).dataType))
      case None => (None, None)
    }
    (columnIndexOption, dataTypeOption)
  }
}
