/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.engine.frame.plugins.bincolumn.DiscretizationFunctions

/**
 * Aggregator for computing histograms using a list of cutoffs.
 *
 * The histogram is a vector containing the percentage of observations found in each bin
 */
case class HistogramAggregator(cutoffs: List[Double], includeLowest: Option[Boolean] = None, strictBinning: Option[Boolean] = None) extends GroupByAggregator {
  require(cutoffs.size >= 2, "At least one bin is required in cutoff array")
  require(cutoffs == cutoffs.sorted, "the cutoff points of the bins must be monotonically increasing")

  /** An array that aggregates the number of elements in each bin */
  override type AggregateType = Array[Double]

  /** The bin number for a column value */
  override type ValueType = Int

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero = Array.ofDim[Double](cutoffs.size - 1)

  /**
   * Get the bin index for the column value based on the cutoffs
   *
   * Strict binning is disabled so values smaller than the first bin are assigned to the first bin,
   * and values larger than the last bin are assigned to the last bin.
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue != null) {
      DiscretizationFunctions.binElement(DataTypes.toDouble(columnValue),
        cutoffs,
        lowerInclusive = includeLowest.getOrElse(true),
        strictBinning = strictBinning.getOrElse(false))
    }
    else -1
  }

  /**
   * Increment the count for the bin corresponding to the bin index
   */
  override def add(binArray: AggregateType, binIndex: ValueType): AggregateType = {
    if (binIndex >= 0) binArray(binIndex) += 1
    binArray
  }

  /**
   * Sum two binned lists.
   */
  override def merge(binArray1: AggregateType, binArray2: AggregateType) = {
    (binArray1, binArray2).zipped.map(_ + _)
  }

  /**
   * Return the vector containing the percentage of observations found in each bin
   */
  override def getResult(binArray: AggregateType): Any = {
    val total = binArray.sum
    if (total > 0) DataTypes.toVector()(binArray.map(_ / total)) else binArray
  }
}
