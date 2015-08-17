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

package org.trustedanalytics.atk.engine.frame.plugins.groupby

import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators._
import org.trustedanalytics.atk.engine.partitioners.SparkCoresPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Computes the aggregated values (Avg, Count, Max, Min, Mean, Sum, Stdev, ...) for specified columns grouped by key.
 *
 * This class uses Spark's aggregateByKey() transformation to compute the aggregated values. aggregateByKey()
 * aggregates the values of each key using an initial "zero value", an operation which merges an input value V into an aggregate value U,
 * and an operation for merging two U's.
 *
 * @see org.apache.spark.rdd.PairRDDFunctions#aggregateByKey
 *
 * @param pairedRDD RDD of group-by keys, and aggregation column values
 * @param columnAggregators List of columns, and corresponding aggregators
 */
case class GroupByAggregateByKey(pairedRDD: RDD[(Seq[Any], Seq[Any])],
                                 columnAggregators: List[ColumnAggregator]) extends Serializable {

  // Scala is not that great at handling unions of types
  // These type definitions specify the lower and upper type bounds for GroupByAggregator so that
  // we can operate on a collection of different types of aggregators
  // These bounds can be interpreted as ( GroupByAggregator is-a aggregator) && (aggregator is-a GroupByAggregator)
  type InputType = (aggregator#ValueType) forSome { type aggregator >: GroupByAggregator <: GroupByAggregator }
  type AggregateType = (aggregator#AggregateType) forSome { type aggregator >: GroupByAggregator <: GroupByAggregator }

  val numColumns = columnAggregators.length

  /**
   * Computes the aggregated values (Avg, Count, Max, Min, Mean, Sum, Stdev, ...) for specified columns grouped by key.
   *
   * @return Row RDD with results of aggregation
   */
  def aggregateByKey(): RDD[Row] = {
    val zeroValues = columnAggregators.map(_.aggregator.zero)
    val numPartitions = SparkCoresPartitioner.getNumPartitions(pairedRDD)

    pairedRDD.map {
      case (key, row) =>
        mapAll(key, row)
    }.aggregateByKey[Seq[Any]](zeroValues, numPartitions)(
      (aggregateValues, columnValues) => addAll(aggregateValues, columnValues),
      (aggregateValues1, aggregateValues2) => mergeAll(aggregateValues1, aggregateValues2)
    ).map {
        case (key, row) =>
          getResults(key, row)
      }
  }

  /**
   * Transforms the column values into the input values expected by the aggregators
   *
   * For example, the map function for the CountAccumulator outputs (key, 1L)
   *
   * @param key Group key
   * @param row Row of column values to aggregate
   * @return Group key, and sequence of input values for aggregator
   */
  private def mapAll(key: Seq[Any],
                     row: Seq[Any]): (Seq[Any], Seq[InputType]) = {
    val seq = columnAggregators.map(colAggregator => {
      val aggregator = colAggregator.aggregator
      aggregator.mapFunction(row(colAggregator.columnIndex), colAggregator.column.dataType)
    })
    (key, seq)
  }

  /**
   * Adds the map values to the corresponding aggregate values
   *
   * @param aggregateValues Sequence of aggregate values for columns (e.g., running counts, averages)
   * @param inputValues Sequence of input values
   * @return  Sequence of updated aggregate values
   */
  private def addAll(aggregateValues: Seq[Any],
                     inputValues: Seq[InputType]): Seq[AggregateType] = {
    var i = 0
    val buf = new ListBuffer[AggregateType]()

    // Using while loops instead of for loops to improve performance
    while (i < numColumns) {
      val aggregator = columnAggregators(i).aggregator
      val aggregateValue = aggregateValues(i).asInstanceOf[aggregator.type#AggregateType]
      val inputValue = inputValues(i).asInstanceOf[aggregator.type#ValueType]
      buf += aggregator.add(aggregateValue, inputValue).asInstanceOf[AggregateType]
      i += 1
    }
    buf.toSeq
  }

  /**
   * Combine aggregate values for a given key from different Spark partitions
   *
   * For example, combining the running counts from different Spark partitions for a given key
   *
   * @param aggregateValues1 Sequence of aggregate values for a given key from one Spark partition
   * @param aggregateValues2 Sequence of aggregate values for a given key from a different Spark partition
   *
   * @return Combined aggregate values
   */
  private def mergeAll(aggregateValues1: Seq[Any],
                       aggregateValues2: Seq[Any]): Seq[AggregateType] = {
    var i = 0
    val buf = new ListBuffer[AggregateType]()

    // Using while loops instead of for loops to improve performance
    while (i < numColumns) {
      val aggregator = columnAggregators(i).aggregator

      // Need to check for empty accumulator values in case one of the Spark partitions had no data
      // Empty Spark partitions can arise if there are too many partitions
      val aggregateValue1 = if (aggregateValues1.isEmpty) aggregator.zero else aggregateValues1(i)
      val aggregateValue2 = if (aggregateValues2.isEmpty) aggregator.zero else aggregateValues2(i)
      buf += aggregator.merge(
        aggregateValue1.asInstanceOf[aggregator.type#AggregateType],
        aggregateValue2.asInstanceOf[aggregator.type#AggregateType]).asInstanceOf[AggregateType]
      i += 1
    }
    buf.toSeq
  }

  /**
   * Get the final results for a given key, e.g., total counts
   *
   * The output of the aggregators might be an intermediate result. For example, for the
   * arithmetic result, the output of the accumulator is the total sum and count. This method
   * computes the final output value (e.g., mean=(sum/count)
   *
   * @param key Row key
   * @param row List of aggregated values for each column
   * @return Row with key and aggregated values
   */
  private def getResults(key: Seq[Any], row: Seq[Any]): Row = {
    var i = 0
    val buf = new ListBuffer[Any]()
    buf ++= key
    // Using while loops instead of for loops to improve performance
    while (i < row.length) {
      val columnValue = row(i)
      val aggregator = columnAggregators(i).aggregator
      buf += aggregator.getResult(columnValue.asInstanceOf[aggregator.type#AggregateType])
      i += 1
    }
    Row.fromSeq(buf)
  }

}
