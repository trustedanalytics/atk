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


package org.trustedanalytics.atk.engine.frame.plugins.bincolumn

import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.SparkContext._

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
 */
abstract class ComputedBinColumnPlugin extends SparkCommandPlugin[ComputedBinColumnArgs, BinColumnResults] {

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
  override def execute(arguments: ComputedBinColumnArgs)(implicit invocation: Invocation): BinColumnResults = {
    val frame: SparkFrame = arguments.frame
    frame.schema.requireColumnIsNumerical(arguments.columnName)
    val binColumnName = arguments.binColumnName.getOrElse(frame.schema.getNewColumnName(arguments.columnName + "_binned"))
    val numBins = HistogramPlugin.getNumBins(arguments.numBins, frame)

    // run the operation and save results
    val columnIndex = frame.schema.columnIndex(arguments.columnName)
    val binnedResults = executeBinColumn(columnIndex, numBins, frame.rdd)
    val updatedSchema = frame.schema.addColumn(binColumnName, DataTypes.int32)
    val result = frame.save(new FrameRdd(updatedSchema, binnedResults.rdd))

    new BinColumnResults(result.entity, binnedResults.cutoffs)
  }

  /**
   * Discretize a variable into a finite number of bins
   * @param columnIndex index of column to bin
   * @param numBins number of bins to use
   * @param rdd rdd to bin against
   * @return a result object containing the binned rdd and the list of computed cutoffs
   */
  def executeBinColumn(columnIndex: Int, numBins: Int, rdd: FrameRdd): RddWithCutoffs

}
