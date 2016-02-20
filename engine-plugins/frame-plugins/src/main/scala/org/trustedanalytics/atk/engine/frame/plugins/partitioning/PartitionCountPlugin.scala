/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.frame.plugins.partitioning

import org.trustedanalytics.atk.domain.IntValue
import org.trustedanalytics.atk.domain.command.CommandDoc
import org.trustedanalytics.atk.domain.frame.FrameNoArgs
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Get the RDD partition count after loading a frame (useful for debugging purposes)
 */
@PluginDoc(oneLine = "Calls underlying Spark RDD method.",
  extended = "",
  returns = "")
class PartitionCountPlugin extends SparkCommandPlugin[FrameNoArgs, IntValue] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/_partition_count"

  /**
   * Get the RDD partition count after loading a frame (useful for debugging purposes)
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: FrameNoArgs)(implicit invocation: Invocation): IntValue = {
    val frame: SparkFrame = arguments.frame
    new IntValue(frame.rdd.partitions.size)
  }
}
