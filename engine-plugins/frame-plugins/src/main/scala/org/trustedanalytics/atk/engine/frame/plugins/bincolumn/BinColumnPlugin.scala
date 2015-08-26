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

package org.trustedanalytics.atk.engine.frame.plugins.bincolumn

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ Schema, DataTypes }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Column values into bins.
 *
 * Two types of binning are provided: equalwidth and equaldepth.
 *
 * Equal width binning places column values into bins such that the values in each bin fall within the same
 * interval and the interval width for each bin is equal.
 *
 * Equal depth binning attempts to place column values into bins such that each bin contains the same number
 * of elements
 *
 */
@PluginDoc(oneLine = "Classify data into user-defined groups.",
  extended = """Summarize rows of data based on the value in a single column by sorting them
into bins, or groups, based on a list of bin cutoff points.

Notes
-----
1)  Unicode in column names is not supported and will likely cause the
    drop_frames() method (and others) to fail!
2)  Bins IDs are 0-index: the lowest bin number is 0.
3)  The first and last cutoffs are always included in the bins.
    When include_lowest is ``True``, the last bin includes both cutoffs.
    When include_lowest is ``False``, the first bin (bin 0) includes both
    cutoffs.""")
class BinColumnPlugin extends SparkCommandPlugin[BinColumnArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/bin_column"

  /**
   * Column values into bins.
   *
   * Two types of binning are provided: equalwidth and equaldepth.
   *
   * Equal width binning places column values into bins such that the values in each bin fall within the same
   * interval and the interval width for each bin is equal.
   *
   * Equal depth binning attempts to place column values into bins such that each bin contains the same number
   * of elements
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: BinColumnArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame
    val columnIndex = frame.schema.columnIndex(arguments.columnName)
    frame.schema.requireColumnIsNumerical(arguments.columnName)
    val binColumnName = arguments.binColumnName.getOrElse(frame.schema.getNewColumnName(arguments.columnName + "_binned"))

    // run the operation and save results
    val updatedSchema = frame.schema.addColumn(binColumnName, DataTypes.int32)
    val binnedRdd = DiscretizationFunctions.binColumns(columnIndex, arguments.cutoffs,
      arguments.includeLowest.getOrElse(true), arguments.strictBinning.getOrElse(false), frame.rdd)

    frame.save(new FrameRdd(updatedSchema, binnedRdd))
  }
}
