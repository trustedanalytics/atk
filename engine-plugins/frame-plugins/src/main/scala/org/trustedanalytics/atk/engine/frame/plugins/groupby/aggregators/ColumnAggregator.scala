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

import org.trustedanalytics.atk.domain.frame.GroupByAggregationArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }

import scala.util.Try

/**
 * Column and corresponding aggregator.
 */
case class ColumnAggregator(column: Column, columnIndex: Int, aggregator: GroupByAggregator)

object ColumnAggregator {

  /**
   * Get the column aggregator for histogram
   *
   * @param aggregationArgs Aggregation arguments
   * @param columnIndex Index of new column
   * @return Column aggregator for histogram
   */
  def getHistogramColumnAggregator(aggregationArgs: GroupByAggregationArgs, columnIndex: Int): ColumnAggregator = {
    val functionName = aggregationArgs.function
    require(functionName.matches("""HISTOGRAM\s*=\s*\{.*\}"""), s"Unsupported aggregation function for histogram: $functionName")

    val newColumnName = aggregationArgs.newColumnName.split("=")(0)
    val histogramAggregator = getHistogramAggregator(functionName.split("=")(1))
    ColumnAggregator(Column(newColumnName, DataTypes.vector(histogramAggregator.cutoffs.length - 1)), columnIndex, histogramAggregator)
  }

  /**
   * Parses the JSON for arguments and creates a new HistogramAggregator
   * @param argsJson json str of object describing histogram args
   * @return new aggregator
   */
  def getHistogramAggregator(argsJson: String): HistogramAggregator = {
    import spray.json._
    import spray.json.DefaultJsonProtocol._

    val jsObject = Try(argsJson.parseJson.asJsObject).getOrElse({
      throw new IllegalArgumentException(s"cutoffs should be valid JSON: $argsJson")
    })
    val cutoffs: List[Double] = jsObject.fields.get("cutoffs") match {
      case Some(x) => Try(x.convertTo[List[Double]]).getOrElse(throw new IllegalArgumentException(s"cutoffs should be numeric"))
      case _ => throw new IllegalArgumentException(s"cutoffs required for group_by histogram")
    }
    val includeLowest: Option[Boolean] = jsObject.fields.get("include_lowest") match {
      case Some(x) => Some(Try(x.convertTo[Boolean]).getOrElse(throw new IllegalArgumentException(s"includeLowest should be boolean")))
      case _ => None
    }
    val strictBinning: Option[Boolean] = jsObject.fields.get("strict_binning") match {
      case Some(x) => Some(Try(x.convertTo[Boolean]).getOrElse(throw new IllegalArgumentException(s"strictBinning should be boolean")))
      case _ => None
    }
    HistogramAggregator(cutoffs, includeLowest, strictBinning)
  }
}
