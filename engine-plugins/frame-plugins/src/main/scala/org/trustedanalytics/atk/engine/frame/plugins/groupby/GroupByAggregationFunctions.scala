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

package org.trustedanalytics.atk.engine.frame.plugins.groupby

import org.trustedanalytics.atk.domain.frame.GroupByAggregationArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema, Schema }
import org.trustedanalytics.atk.engine.frame.plugins.groupby.aggregators._
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD

/**
 * Aggregations for Frames (SUM, COUNT, etc)
 *
 * This is a wrapper to encapsulate methods that may need to be serialized to executed on Spark worker nodes.
 * If you don't know what this means please read about Closure Mishap
 * [[http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf]]
 * and Task Serialization
 * [[http://stackoverflow.com/questions/22592811/scala-spark-task-not-serializable-java-io-notserializableexceptionon-when]]
 */
private object GroupByAggregationFunctions extends Serializable {

  /**
   * Create a Summarized Frame with Aggregations (Avg, Count, Max, Min, ...).
   *
   * For example, grouping a frame by gender and age, and computing the average income.
   *
   * New aggregations can be added by implementing a GroupByAggregator.
   *
   * @see GroupByAggregator
   *
   * @param frameRdd Input frame
   * @param groupByColumns List of columns to group by
   * @param aggregationArguments List of aggregation arguments
   * @return Summarized frame with aggregations
   */
  def aggregation(frameRdd: FrameRdd,
                  groupByColumns: List[Column],
                  aggregationArguments: List[GroupByAggregationArgs]): FrameRdd = {

    val frameSchema = frameRdd.frameSchema
    val columnAggregators = createColumnAggregators(frameSchema, aggregationArguments)

    val pairedRowRDD = pairRowsByGroupByColumns(frameRdd, groupByColumns, aggregationArguments)

    val aggregationRDD = GroupByAggregateByKey(pairedRowRDD, columnAggregators).aggregateByKey()

    val newColumns = groupByColumns ++ columnAggregators.map(_.column)
    val newSchema = FrameSchema(newColumns)

    new FrameRdd(newSchema, aggregationRDD)
  }

  /**
   * Returns a list of columns and corresponding accumulators used to aggregate values
   *
   * @param aggregationArguments List of aggregation arguments (i.e., aggregation function, column, new column name)
   * @param frameSchema Frame schema
   * @return  List of columns and corresponding accumulators
   */
  def createColumnAggregators(frameSchema: Schema, aggregationArguments: List[(GroupByAggregationArgs)]): List[ColumnAggregator] = {

    aggregationArguments.zipWithIndex.map {
      case (arg, i) =>
        val column = frameSchema.column(arg.columnName)

        arg.function match {
          case "COUNT" =>
            ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, CountAggregator())
          case "COUNT_DISTINCT" =>
            ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, DistinctCountAggregator())
          case "MIN" =>
            ColumnAggregator(Column(arg.newColumnName, column.dataType), i, MinAggregator())
          case "MAX" =>
            ColumnAggregator(Column(arg.newColumnName, column.dataType), i, MaxAggregator())
          case "SUM" if column.dataType.isNumerical =>
            if (column.dataType.isInteger)
              ColumnAggregator(Column(arg.newColumnName, DataTypes.int64), i, new SumAggregator[Long]())
            else
              ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, new SumAggregator[Double]())
          case "AVG" if column.dataType.isNumerical =>
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, MeanAggregator())
          case "VAR" if column.dataType.isNumerical =>
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, VarianceAggregator())
          case "STDEV" if column.dataType.isNumerical =>
            ColumnAggregator(Column(arg.newColumnName, DataTypes.float64), i, StandardDeviationAggregator())
          case function if function.matches("""HISTOGRAM.*""") && column.dataType.isNumerical =>
            ColumnAggregator.getHistogramColumnAggregator(arg, i)
          case _ => throw new IllegalArgumentException(s"Unsupported aggregation function: ${arg.function} for data type: ${column.dataType}")
        }
    }
  }

  /**
   * Create a pair RDD using the group-by keys, and aggregation columns
   *
   * The group-by key is a sequence of column values, for example, group-by gender and age. The aggregation
   * columns are the columns containing the values to be aggregated, for example, annual income.
   *
   * @param frameRdd Input frame
   * @param groupByColumns Group by columns
   * @param aggregationArguments List of aggregation arguments
   * @return RDD of group-by keys, and aggregation column values
   */
  def pairRowsByGroupByColumns(frameRdd: FrameRdd,
                               groupByColumns: List[Column],
                               aggregationArguments: List[GroupByAggregationArgs]): RDD[(Seq[Any], Seq[Any])] = {
    val frameSchema = frameRdd.frameSchema
    val groupByColumnsNames = groupByColumns.map(col => col.name)

    val aggregationColumns = aggregationArguments.map(arg => frameSchema.column(columnName = arg.columnName))

    frameRdd.mapRows(row => {
      val groupByKey = if (groupByColumnsNames.nonEmpty) row.valuesAsArray(groupByColumnsNames).toSeq else Seq[Any]()
      val groupByRow = aggregationColumns.map(col => row.data(frameSchema.columnIndex(col.name)))
      (groupByKey, groupByRow.toSeq)
    })
  }

}
