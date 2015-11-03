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


package org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 *  Aggregator for computing the maximum column values using Spark's aggregateByKey()
 *
 *  Supports any data type that is comparable.
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
case class MaxAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = Any

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Any

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: Any = null

  /**
   * Outputs column value
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = columnValue

  /**
   * Returns the maximum of the two input parameters.
   */
  override def add(max: AggregateType, mapValue: ValueType): AggregateType = getMaximum(max, mapValue)

  /**
   * Returns the maximum of the two input parameters.
   */
  override def merge(max1: AggregateType, max2: AggregateType) = getMaximum(max1, max2)

  /**
   * Returns the maximum value for data types that are comparable
   */
  private def getMaximum(left: Any, right: Any): Any = {
    if (left != null && DataTypes.compare(left, right) >= 0) // Ignoring nulls
      left
    else right
  }

}
