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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives

import org.trustedanalytics.atk.domain.frame.{ ColumnFullStatisticsReturn, ColumnMedianReturn, ColumnModeReturn, ColumnSummaryStatisticsReturn }
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.engine.frame.plugins.statistics.numericalstatistics._
import org.trustedanalytics.atk.engine.frame.plugins.statistics.{ FrequencyStatistics, OrderStatistics }
import org.apache.spark.rdd.RDD
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
 * Provides functions for taking statistics on column data.
 */
object ColumnStatistics extends Serializable {

  /**
   * Calculate (weighted) mode of a data column, the weight of the mode, and the total weight of the column.
   * A mode is a value that has maximum weight. Ties are resolved arbitrarily.
   * Values with non-positive weights (including NaNs and infinite values) are thrown out before the calculation is
   * performed.
   *
   * When the total weight is 0, the option None is given for the mode and the weight of the mode.
   *
   * @param dataColumnIndex Index of the column providing data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param modeCountOption Option for the maximum number of modes returned. Defaults to 1.
   * @param rowRDD RDD of input rows.
   * @return The mode of the column (as a string), the weight of the mode, and the total weight of the data.
   */
  def columnMode(dataColumnIndex: Int,
                 dataType: DataType,
                 weightsColumnIndexOption: Option[Int],
                 weightsTypeOption: Option[DataType],
                 modeCountOption: Option[Int],
                 rowRDD: RDD[Row]): ColumnModeReturn = {

    val defaultNumberOfModesReturned = 1

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    val modeCount = modeCountOption.getOrElse(defaultNumberOfModesReturned)

    val frequencyStatistics = new FrequencyStatistics(dataWeightPairs, modeCount)

    val modeSet = frequencyStatistics.modeSet

    val modeSetJsValue = modeSet.map(x => dataType.typedJson(x)).toJson

    ColumnModeReturn(modeSetJsValue,
      frequencyStatistics.weightOfMode,
      frequencyStatistics.totalWeight,
      frequencyStatistics.modeCount)
  }

  /**
   * Calculate the median of a data column containing numerical data. The median is the least value X in the range of the
   * distribution so that the cumulative weight strictly below X is < 1/2  the total weight and the cumulative
   * distribution up to and including X is >= 1/2 the total weight.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed. The option None is returned when the total weight is 0.
   *
   * @param dataColumnIndex Index of the data column.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption  Option for index of column providing  weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @return The  median of the column.
   */
  def columnMedian(dataColumnIndex: Int,
                   dataType: DataType,
                   weightsColumnIndexOption: Option[Int],
                   weightsTypeOption: Option[DataType],
                   rowRDD: RDD[Row]): ColumnMedianReturn = {

    val dataWeightPairs: RDD[(Any, Double)] =
      getDataWeightPairs(dataColumnIndex, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    implicit val ordering: Ordering[Any] = new NumericalOrdering(dataType)

    val orderStatistics = new OrderStatistics[Any](dataWeightPairs)

    val medianReturn: JsValue = if (orderStatistics.medianOption.isEmpty) {
      JsNull
    }
    else {
      dataType.typedJson(orderStatistics.medianOption.get)
    }
    ColumnMedianReturn(medianReturn)
  }

  private class NumericalOrdering(dataType: DataType) extends Ordering[Any] {
    override def compare(x: Any, y: Any): Int = {
      dataType.asDouble(x).compareTo(dataType.asDouble(y))
    }
  }

  /**
   * Calculate summary statistics of data column, possibly weighted by an optional weights column.
   *
   * Values with non-positive weights(including NaNs and infinite values) are thrown out before the calculation is
   * performed, however, they are logged as "bad rows" (when a row contain a datum or a weight that is not a finite
   * number) or as "non positive weight" (when a row's weight entry is <= 0).
   *
   * @param dataColumnIndex Index of column providing the data. Must be numerical data.
   * @param dataType The type of the data column.
   * @param weightsColumnIndexOption Option for index of column providing the weights. Must be numerical data.
   * @param weightsTypeOption Option for the datatype of the weights.
   * @param rowRDD RDD of input rows.
   * @param usePopulationVariance If true, variance is calculated as population variance. If false, variance is
   *                              calculated as sample variance.
   * @return Summary statistics of the column.
   */
  def columnSummaryStatistics(dataColumnIndex: Int,
                              dataType: DataType,
                              weightsColumnIndexOption: Option[Int],
                              weightsTypeOption: Option[DataType],
                              rowRDD: RDD[Row],
                              usePopulationVariance: Boolean): ColumnSummaryStatisticsReturn = {

    val dataWeightPairs: RDD[(Option[Double], Option[Double])] =
      getDoubleWeightPairs(dataColumnIndex, dataType, weightsColumnIndexOption, weightsTypeOption, rowRDD)

    val stats = new NumericalStatistics(dataWeightPairs, usePopulationVariance)

    ColumnSummaryStatisticsReturn(mean = stats.weightedMean,
      geometricMean = stats.weightedGeometricMean,
      variance = stats.weightedVariance,
      standardDeviation = stats.weightedStandardDeviation,
      totalWeight = stats.totalWeight,
      meanConfidenceLower = stats.meanConfidenceLower,
      meanConfidenceUpper = stats.meanConfidenceUpper,
      minimum = stats.min,
      maximum = stats.max,
      positiveWeightCount = stats.positiveWeightCount,
      nonPositiveWeightCount = stats.nonPositiveWeightCount,
      badRowCount = stats.badRowCount,
      goodRowCount = stats.goodRowCount)
  }

  def getDataWeightPairs(dataColumnIndex: Int,
                         weightsColumn: Option[Column],
                         rowRDD: RDD[Row]): RDD[(Any, Double)] = {
    weightsColumn match {
      case Some(column) => getDataWeightPairs(dataColumnIndex, Some(column.index), Some(column.dataType), rowRDD)
      case None => getDataWeightPairs(dataColumnIndex, None, None, rowRDD)
    }
  }

  def getDataWeightPairs(dataColumnIndex: Int,
                         weightsColumnIndexOption: Option[Int],
                         weightsTypeOption: Option[DataType],
                         rowRDD: RDD[Row]): RDD[(Any, Double)] = {

    val dataRDD: RDD[Any] = rowRDD.map(row => row(dataColumnIndex))

    val weighted = weightsColumnIndexOption.isDefined

    if (weightsColumnIndexOption.nonEmpty && weightsTypeOption.isEmpty) {
      throw new IllegalArgumentException("Cannot specify weights column without specifying its datatype.")
    }

    val weightsRDD = if (weighted)
      rowRDD.map(row => weightsTypeOption.get.asDouble(row(weightsColumnIndexOption.get)))
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, 1.toDouble))
  }

  private def getDoubleWeightPairs(dataColumnIndex: Int,
                                   dataType: DataType,
                                   weightsColumnIndexOption: Option[Int],
                                   weightsTypeOption: Option[DataType],
                                   rowRDD: RDD[Row]): RDD[(Option[Double], Option[Double])] = {

    val dataRDD: RDD[Option[Double]] = rowRDD.map {
      case row => extractColumnValueAsDoubleFromRow(row, dataColumnIndex, dataType)
    }

    val weighted = weightsColumnIndexOption.isDefined

    if (weightsColumnIndexOption.nonEmpty && weightsTypeOption.isEmpty) {
      throw new IllegalArgumentException("Cannot specify weights column without specifying its datatype.")
    }

    val weightsRDD = if (weighted)
      rowRDD.map {
        case row => extractColumnValueAsDoubleFromRow(row, weightsColumnIndexOption.get, weightsTypeOption.get)
      }
    else
      null

    if (weighted) dataRDD.zip(weightsRDD) else dataRDD.map(x => (x, Some(1.toDouble)))
  }

  private def extractColumnValueAsDoubleFromRow(row: Row, columnIndex: Int, dataType: DataType): Option[Double] = {
    val columnValue = row(columnIndex)
    columnValue match {
      case null => None
      case value => Some(dataType.asDouble(value))
    }
  }

}
