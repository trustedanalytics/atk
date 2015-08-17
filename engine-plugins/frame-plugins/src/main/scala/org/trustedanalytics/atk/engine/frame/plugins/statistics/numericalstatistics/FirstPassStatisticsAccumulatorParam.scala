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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.numericalstatistics

import org.apache.spark.AccumulatorParam

/**
 * Accumulator param for combiningin FirstPassStatistics.
 */
private[numericalstatistics] class FirstPassStatisticsAccumulatorParam
    extends AccumulatorParam[FirstPassStatistics] with Serializable {

  override def zero(initialValue: FirstPassStatistics) =
    FirstPassStatistics(mean = 0,
      weightedSumOfSquares = 0,
      weightedSumOfSquaredDistancesFromMean = 0,
      weightedSumOfLogs = Some(0),
      minimum = Double.PositiveInfinity,
      maximum = Double.NegativeInfinity,
      totalWeight = 0,
      positiveWeightCount = 0,
      nonPositiveWeightCount = 0,
      badRowCount = 0,
      goodRowCount = 0)

  override def addInPlace(stats1: FirstPassStatistics, stats2: FirstPassStatistics): FirstPassStatistics = {

    val totalWeight = stats1.totalWeight + stats2.totalWeight

    val mean = if (totalWeight > BigDecimal(0))
      (stats1.mean * stats1.totalWeight + stats2.mean * stats2.totalWeight) / totalWeight
    else
      BigDecimal(0)

    val weightedSumOfSquares = stats1.weightedSumOfSquares + stats2.weightedSumOfSquares

    val sumOfSquaredDistancesFromMean =
      weightedSumOfSquares - BigDecimal(2) * mean * mean * totalWeight + mean * mean * totalWeight

    val weightedSumOfLogs: Option[BigDecimal] =
      if (stats1.weightedSumOfLogs.nonEmpty && stats2.weightedSumOfLogs.nonEmpty) {
        Some(stats1.weightedSumOfLogs.get + stats2.weightedSumOfLogs.get)
      }
      else {
        None
      }

    FirstPassStatistics(mean = mean,
      weightedSumOfSquares = weightedSumOfSquares,
      weightedSumOfSquaredDistancesFromMean = sumOfSquaredDistancesFromMean,
      weightedSumOfLogs = weightedSumOfLogs,
      minimum = Math.min(stats1.minimum, stats2.minimum),
      maximum = Math.max(stats1.maximum, stats2.maximum),
      totalWeight = totalWeight,
      positiveWeightCount = stats1.positiveWeightCount + stats2.positiveWeightCount,
      nonPositiveWeightCount = stats1.nonPositiveWeightCount + stats2.nonPositiveWeightCount,
      badRowCount = stats1.badRowCount + stats2.badRowCount,
      goodRowCount = stats1.goodRowCount + stats2.goodRowCount)
  }
}
