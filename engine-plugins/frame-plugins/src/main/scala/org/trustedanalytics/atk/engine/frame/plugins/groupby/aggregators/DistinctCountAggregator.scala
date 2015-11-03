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

import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 * Aggregator for counting distinct column values.
 *
 * This aggregator assumes that the number of distinct values for a given key can fit within memory.
 */
case class DistinctCountAggregator() extends GroupByAggregator {

  /** Type for aggregate values that corresponds to type U in Spark's aggregateByKey() */
  override type AggregateType = Set[Any]

  /** Output type of the map function that corresponds to type V in Spark's aggregateByKey() */
  override type ValueType = Any

  /** The 'empty' or 'zero' or default value for the aggregator */
  override def zero = Set.empty[Any]

  /**
   * Outputs column value
   */
  override def mapFunction(columnValue: Any, columnDataType: DataType): ValueType = columnValue

  /**
   * Add map value to set which stores distinct column values.
   */
  override def add(set: AggregateType, mapValue: ValueType): AggregateType = set + mapValue

  /**
   * Merge two sets.
   */
  override def merge(set1: AggregateType, set2: AggregateType) = set1 ++ set2

  /**
   * Returns count of distinct column values
   */
  override def getResult(set: AggregateType): Any = set.size.toLong // toLong needed to avoid casting errors when writing to Parquet
}
