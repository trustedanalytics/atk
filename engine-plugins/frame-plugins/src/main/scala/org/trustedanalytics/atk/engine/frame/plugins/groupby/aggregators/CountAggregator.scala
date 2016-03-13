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

import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 *  Aggregator for counting column values.
 */
case class CountAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = Long

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Long

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero = 0L

  /**
   * Outputs 'one' for each column value
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = 1L

  /**
   * Increments count by map value.
   */
  override def add(count: AggregateType, mapValue: ValueType): AggregateType = count + mapValue

  /**
   * Sums two counts.
   */
  override def merge(count1: AggregateType, count2: AggregateType) = count1 + count2
}
