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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.frame.CumulativePercentArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Compute a cumulative percent sum.
 *
 */
@PluginDoc(oneLine = "Add column to frame with cumulative percent sum.",
  extended = """A cumulative percent sum is computed by sequentially stepping through the
column values and keeping track of the current percentage of the total sum
accounted for at the current value.


Notes
-----
This method applies only to columns containing numerical data.
Although this method will execute for columns containing negative
values, the interpretation of the result will change (for example,
negative percentages).""")
class CumulativePercentPlugin extends SparkCommandPlugin[CumulativePercentArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/cumulative_percent"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Compute a cumulative percent sum.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CumulativePercentArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame

    // run the operation
    val cumulativeDistRdd = CumulativeDistFunctions.cumulativePercentSum(frame.rdd, arguments.sampleCol)
    val updatedSchema = frame.schema.addColumnFixName(Column(arguments.sampleCol + "_cumulative_percent", DataTypes.float64))

    // save results
    frame.save(new FrameRdd(updatedSchema, cumulativeDistRdd))
  }
}
