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

package org.trustedanalytics.atk.engine.frame.plugins.dotproduct

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.schema.{ Schema, DataTypes }
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

/** Json conversion for arguments and return value case classes */
object DotProductJsonFormat {
  implicit val dotProductFormat = jsonFormat6(DotProductArgs)
}

import DotProductJsonFormat._

/**
 * Plugin that calculates the dot product for each row in a frame.
 *
 * This is an experimental plugin used by the Netflow POC for scoring. The plugin should be revisited
 * once we support lists as data types.
 *
 */
@PluginDoc(oneLine = "Calculate dot product for each row in current frame.",
  extended = """Calculate the dot product for each row in a frame using values from two
equal-length sequences of columns.

Dot product is computed by the following formula:

The dot product of two vectors ``A=[a_1, a_2, ..., a_n]`` and
``B =[b_1, b_2, ..., b_n]`` is ``a_1*b_1 + a_2*b_2 + ...+ a_n*b_n``.
The dot product for each row is stored in a new column in the existing frame.

Notes
-----
If default_left_values or default_right_values are not specified, any null
values will be replaced by zeros.""")
class DotProductPlugin extends SparkCommandPlugin[DotProductArgs, UnitReturn] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/dot_product"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: DotProductArgs)(implicit invocation: Invocation) = 3

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Calculates the dot product for each row in a frame using values from two equal-length sequences of columns.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running dot-product plugin
   * @return Updated frame with dot product stored in new column
   */
  override def execute(arguments: DotProductArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: SparkFrame = arguments.frame

    // run the operation
    val dotProductRdd = DotProductFunctions.dotProduct(frame.rdd, arguments.leftColumnNames, arguments.rightColumnNames,
      arguments.defaultLeftValues, arguments.defaultRightValues)

    // save results
    val updatedSchema = frame.schema.addColumn(arguments.dotProductColumnName, DataTypes.float64)
    frame.save(new FrameRdd(updatedSchema, dotProductRdd))
    UnitReturn()
  }

}
