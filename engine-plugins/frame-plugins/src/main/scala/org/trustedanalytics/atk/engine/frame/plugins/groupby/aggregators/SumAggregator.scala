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

package org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators

import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 *  Aggregator for computing the sum of column values using Spark's aggregateByKey()
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
class SumAggregator[T: Numeric] extends GroupByAggregator {

  val num = implicitly[Numeric[T]]

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = T

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = T

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero: T = num.zero

  /**
   * Converts column value to a Numeric
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = {
    if (columnValue == null) {
      num.zero
    }
    else if (columnDataType.isInteger) {
      DataTypes.toLong(columnValue).asInstanceOf[ValueType]
    }
    else {
      DataTypes.toDouble(columnValue).asInstanceOf[ValueType]
    }
  }

  /**
   * Adds map value to sum
   */
  override def add(sum: AggregateType, mapValue: ValueType): T = num.plus(sum, mapValue)

  /**
   * Adds two sums
   */
  override def merge(sum1: AggregateType, sum2: AggregateType) = num.plus(sum1, sum2)
}
