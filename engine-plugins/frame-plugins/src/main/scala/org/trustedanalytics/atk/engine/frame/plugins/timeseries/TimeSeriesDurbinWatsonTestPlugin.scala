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
import org.trustedanalytics.atk.domain.DoubleValue

/** Json conversion for arguments and return value case classes */
object TimeSeriesDurbinWatsonTestJsonFormat {
  implicit val durbinWatsonTestFormat = jsonFormat2(TimeSeriesDurbinWatsonTestArgs)
}

// Implicits needed for JSON conversion
import TimeSeriesDurbinWatsonTestJsonFormat._

/**
 * Calculate the Durbin-Watson test statistic
 */
@PluginDoc(oneLine = "Durbin-Watson statistics test",
  extended =
    """Computes the Durbin-Watson test statistic used to determine the presence of serial correlation in
      |the residuals. Serial correlation can show a relationship between values separated from each other
      |by a given time lag. A value close to 0.0 gives evidence for positive serial correlation, a value
      |close to 4.0 gives evidence for negative serial correlation, and a value close to 2.0 gives evidence
      |for no serial correlation.""".stripMargin)
class TimeSeriesDurbinWatsonTestPlugin extends SparkCommandPlugin[TimeSeriesDurbinWatsonTestArgs, DoubleValue] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/timeseries_durbin_watson_test"

  /**
   * Returns the Durbin-Watson test statistic
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: TimeSeriesDurbinWatsonTestArgs)(implicit invocation: Invocation): DoubleValue = {
    val frame: SparkFrame = arguments.frame
    DoubleValue(TimeSeriesStatisticalTests.dwtest(TimeSeriesFunctions.getVectorFromFrame(frame.rdd, arguments.residuals)))
  }
}