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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.correlation

import org.trustedanalytics.atk.domain.DoubleValue
import org.trustedanalytics.atk.domain.frame.CorrelationArgs
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate correlation for the specified columns
 */
@PluginDoc(oneLine = "Calculate correlation for two columns of current frame.",
  extended = """Notes
-----
This method applies only to columns containing numerical data.""",
  returns = "Pearson correlation coefficient of the two columns.")
class CorrelationPlugin extends SparkCommandPlugin[CorrelationArgs, DoubleValue] {

  /**
   * The name of the command
   */
  override def name: String = "frame/correlation"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CorrelationArgs)(implicit invocation: Invocation) = 5

  /**
   * Calculate correlation for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for correlation
   * @return value of type declared as the Return type
   */
  override def execute(arguments: CorrelationArgs)(implicit invocation: Invocation): DoubleValue = {

    val frame: SparkFrame = arguments.frame
    frame.schema.validateColumnsExist(arguments.dataColumnNames)
    CorrelationFunctions.correlation(frame.rdd, arguments.dataColumnNames)
  }

}
