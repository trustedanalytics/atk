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

/**
 * Counter used to compute the arithmetic mean incrementally.
 */
case class MeanCounter(count: Long, sum: Double) {
  require(count >= 0, "Count should be greater than zero")
}

/**
 *  Aggregator for incrementally computing the mean column value using Spark's aggregateByKey()
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
case class MeanAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = MeanCounter

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Double

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: MeanCounter = MeanCounter(0L, 0d)

  /**
   * Converts column value to Double
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue != null)
      DataTypes.toDouble(columnValue)
    else
      Double.NaN
  }

  /**
   * Adds map value to incremental mean counter
   */
  override def add(mean: AggregateType, mapValue: ValueType): AggregateType = {
    if (mapValue.isNaN) { // omit value from calculation
      //TODO: Log to IAT EventContext once we figure out how to pass it to Spark workers
      println(s"WARN: Omitting NaNs from mean calculation in group-by")
      mean
    }
    else {
      val sum = mean.sum + mapValue
      val count = mean.count + 1L
      MeanCounter(count, sum)
    }
  }

  /**
   * Merge two mean counters
   */
  override def merge(mean1: AggregateType, mean2: AggregateType) = {
    val count = mean1.count + mean2.count
    val sum = mean1.sum + mean2.sum
    MeanCounter(count, sum)
  }
  override def getResult(mean: AggregateType): Any = if (mean.count > 0) {
    mean.sum / mean.count
  }
  else {
    null //TODO: Re-visit when data types support Inf and NaN
  }

}
