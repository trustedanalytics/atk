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

package org.trustedanalytics.atk.engine.frame.plugins.timeseries

import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

/** Json conversion for arguments and return value case classes */
object TimeSeriesBreuschPaganTestJsonFormat {
  implicit val breuschPaganTestFormat = jsonFormat3(TimeSeriesBreuschPaganTestArgs)
  implicit val breuschPaganTestReturn = jsonFormat2(TimeSeriesBreuschPaganTestReturn)
}

// Implicits needed for JSON conversion
import TimeSeriesBreuschPaganTestJsonFormat._

/**
 * Calculate the Breusch-Pagan test statistic
 */
@PluginDoc(oneLine = "Breusch-Pagan statistics test",
  extended =
    """Performs the Breusch-Pagan test for heteroskedasticity.""")
class TimeSeriesBreuschPaganTestPlugin extends SparkCommandPlugin[TimeSeriesBreuschPaganTestArgs, TimeSeriesBreuschPaganTestReturn] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/timeseries_breusch_pagan_test"

  /**
   * Calculates the Breusch-Pagan test statistic
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesBreuschPaganTestArgs)(implicit invocation: Invocation): TimeSeriesBreuschPaganTestReturn = {
    val frame: SparkFrame = arguments.frame
    val (vector, matrix) = TimeSeriesFunctions.getSparkVectorYAndXFromFrame(frame.rdd, arguments.residuals, arguments.factors)

    val result = TimeSeriesStatisticalTests.bptest(vector, matrix)
    TimeSeriesBreuschPaganTestReturn(result._1, result._2)
  }
}

/**
 * Return value for the Breusch-Pagan Test
 * @param testStat Breusch-Pagan test statistic
 * @param pValue p-value
 */
case class TimeSeriesBreuschPaganTestReturn(testStat: Double, pValue: Double)