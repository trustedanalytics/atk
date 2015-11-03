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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.descriptives

import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Build summary of the data.",
  extended = """Optional parameters:

    top_k *: int*
        Displays levels which are in the top k most frequently
        occurring values for that column.
        Default is 10.

    threshold *: float*
        Displays levels which are above the threshold percentage with
        respect to the total row count.
        Default is 0.0.

Compute a summary of the data in a column(s) for categorical or numerical data types.
The returned value is a Map containing categorical summary for each specified column.

For each column, levels which satisfy the top k and/or threshold cutoffs are
displayed along with their frequency and percentage occurrence with respect to
the total rows in the dataset.

Performs level pruning first based on top k and then filters
out levels which satisfy the threshold criterion.

Missing data is reported when a column value is empty ("") or null.

All remaining data is grouped together in the Other category and its frequency
and percentage are reported as well.

User must specify the column name and can optionally specify top_k and/or threshold.""",
  returns = "Summary for specified column(s) consisting of levels with their frequency and percentage.")
class CategoricalSummaryPlugin extends SparkCommandPlugin[CategoricalSummaryArgs, CategoricalSummaryReturn] {

  private val defaultTopK = 10
  private val defaultThreshold = 0.0

  /**
   * The name of the command, e.g. frame/categorical_summary
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/categorical_summary"

  /**
   * Calculate categorical summary of the specified column.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments Input specification for categorical  summary.
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CategoricalSummaryArgs)(implicit invocation: Invocation): CategoricalSummaryReturn = {

    val frame: SparkFrame = arguments.frame

    // Select each column and invoke summary statistics
    val selectedRdds: List[(FrameRdd, CategoricalColumnInput)] =
      arguments.columnInput.map(elem => (frame.rdd.selectColumn(elem.column), elem))

    val output = for { rdd <- selectedRdds }
      yield CategoricalSummaryImpl.getSummaryStatistics(
      rdd._1,
      frame.rowCount.get.asInstanceOf[Double],
      rdd._2.topK,
      rdd._2.threshold,
      defaultTopK,
      defaultThreshold)

    CategoricalSummaryReturn(output)
  }

}
