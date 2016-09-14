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

package org.trustedanalytics.atk.engine.frame.plugins.boxcox

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.FilterArgs
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema.{ Schema, DataTypes }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

import BoxCoxJsonFormat._

/**
 * Plugin that calculates the box-cox transformation for each row in a frame.
 */
@PluginDoc(oneLine = "Calculate the box-cox transformation for each row in current frame.",
  extended = """Calculate the box-cox transformation for each row in a frame using the given lambda value or default 0.0.

The box-cox transformation is computed by the following formula, where yt is a single entry value(row):

 wt = log(yt); if lambda=0,
 wt = (yt^lambda -1)/lambda ; else

where log is the natural log.""")
class BoxCoxPlugin extends SparkCommandPlugin[BoxCoxArgs, UnitReturn] {
  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/box_cox"

  /**
   * Calculates the box-cox transformation for each row in a frame using the specified lambda value
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running dot-product plugin
   * @return Updated frame with dot product stored in new column
   */
  override def execute(arguments: BoxCoxArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame

    // run the operation
    val boxCoxRdd = BoxCoxFunctions.boxCox(frame.rdd, arguments.columnName, arguments.lambdaValue)

    // save results
    val updatedSchema = frame.schema.addColumn(arguments.boxCoxColumnName.getOrElse(arguments.columnName + "_lambda_" + arguments.lambdaValue.toString), DataTypes.float64)
    frame.save(new FrameRdd(updatedSchema, boxCoxRdd))
  }

}

