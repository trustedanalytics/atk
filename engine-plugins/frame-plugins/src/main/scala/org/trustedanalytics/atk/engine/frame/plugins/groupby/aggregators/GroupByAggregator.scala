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
 *  Trait for group-by aggregators
 *
 *  This trait defines methods needed to implement aggregators, e.g. counts and sums.
 *
 *  This implementation uses Spark's aggregateByKey(). aggregateByKey() aggregates the values of each key using
 *  an initial "zero value", an operation which merges an input value V into an aggregate value U,
 *  and an operation for merging two U's.
 *
 *  @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 */
trait GroupByAggregator extends Serializable {

  /**
   * The type that represents aggregate results. For example, Double for mean, and Long for distinct count values.
   */
  type AggregateType

  /**
   * A type that represents the value to be aggregated. For example, 'ones' for counts, or column values for sums
   */
  type ValueType

  /**
   * The 'empty' or 'zero' or initial value for the aggregator
   */
  def zero: AggregateType

  /**
   * Map function that transforms column values in each row into the input expected by the aggregator
   *
   * For example, for counts, the map function would output 'one' for each map value.
   *
   * @param columnValue Column value
   * @param columnDataType Column data type
   * @return Input value for aggregator
   */
  def mapFunction(columnValue: Any, columnDataType: DataType): ValueType

  /**
   * Adds the output of the map function to the aggregate value
   *
   * This function increments the aggregated value within a single Spark partition.
   *
   * @param aggregateValue Current aggregate value
   * @param mapValue Input value
   * @return Updated aggregate value
   */
  def add(aggregateValue: AggregateType, mapValue: ValueType): AggregateType

  /**
   * Combines two aggregate values
   *
   * This function combines aggregate values across Spark partitions.
   * For example, adding two counts
   *
   * @param aggregateValue1 First aggregate value
   * @param aggregateValue2 Second aggregate value
   * @return Combined aggregate value
   */
  def merge(aggregateValue1: AggregateType, aggregateValue2: AggregateType): AggregateType

  /**
   * Returns the results of the aggregator
   *
   * For some aggregators, this involves casting the result to a Scala Any type.
   * Other aggregators output an intermediate value, so this method computes the
   * final result. For example, the arithmetic mean is represented as a count and sum,
   * so this method computes the mean by dividing the count by the sum.
   *
   * @param result Results of aggregator (might be an intermediate value)
   * @return Final result
   */
  def getResult(result: AggregateType): Any = result

}
