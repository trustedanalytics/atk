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


package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/** Arguments to GroupByPlugin (see Spark API) */
case class GroupByArgs(frame: FrameReference,
                       @ArgDoc("""list of columns to group on""") groupByColumns: List[String],
                       @ArgDoc("""the aggregations to perform""") aggregations: List[GroupByAggregationArgs]) {
  require(frame != null, "frame is required")
  require(groupByColumns != null, "group_by columns is required")
  require(aggregations != null, "aggregation list is required")
}

/**
 * Arguments for GroupBy aggregation
 *
 * @param function Name of aggregation function (e.g., count, sum, variance)
 * @param columnName Name of column to aggregate
 * @param newColumnName Name of new column that stores the aggregated results
 */
case class GroupByAggregationArgs(function: String, columnName: String, newColumnName: String)
