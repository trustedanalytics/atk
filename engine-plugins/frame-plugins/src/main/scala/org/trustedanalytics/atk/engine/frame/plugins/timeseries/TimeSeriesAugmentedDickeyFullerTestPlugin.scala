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
object TimeSeriesAugmentedDickeyFullerTestJsonFormat {
  implicit val augmentedDickeyFullerTestFormat = jsonFormat4(TimeSeriesAugmentedDickeyFullerTestArgs)
  implicit val augmentedDickeyFullerTestReturn = jsonFormat2(TimeSeriesAugmentedDickeyFullerTestReturn)
}

// Implicits needed for JSON conversion
import TimeSeriesAugmentedDickeyFullerTestJsonFormat._

/**
 * Calculate the Augmented Dickey-Fuller test statistic
 */
@PluginDoc(oneLine = "Augmented Dickey-Fuller statistics test",
  extended =
    """Performs the Augmented Dickey-Fuller (ADF) Test, which tests the null hypothesis of whether a unit root is
      |present in a time series sample. The test statistic that is returned in a negative number. The lower the value,
      |the stronger the rejection of the hypothesis that there is a unit root at some level of confidence.""".stripMargin)
class TimeSeriesAugmentedDickeyFullerTestPlugin extends SparkCommandPlugin[TimeSeriesAugmentedDickeyFullerTestArgs, TimeSeriesAugmentedDickeyFullerTestReturn] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/timeseries_augmented_dickey_fuller_test"

  /**
   * Returns the Augmented Dickey-Fuller test statistic
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesAugmentedDickeyFullerTestArgs)(implicit invocation: Invocation): TimeSeriesAugmentedDickeyFullerTestReturn = {
    val frame: SparkFrame = arguments.frame
    val tsVector = TimeSeriesFunctions.getVectorFromFrame(frame.rdd, arguments.tsColumn)
    val dftResult = TimeSeriesStatisticalTests.adftest(tsVector, arguments.maxLag, arguments.regression)
    TimeSeriesAugmentedDickeyFullerTestReturn(dftResult._1, dftResult._2)
  }
}

/**
 * Return value for the Augmented Dickey-Fuller test
 * @param testStat Dickey-Fuller test statistic
 * @param pValue p-value
 */
case class TimeSeriesAugmentedDickeyFullerTestReturn(testStat: Double, pValue: Double)