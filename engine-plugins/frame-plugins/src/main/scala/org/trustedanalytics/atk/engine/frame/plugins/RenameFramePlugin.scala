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
package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.domain.frame.{ FrameName, FrameReference }
import org.trustedanalytics.atk.engine.frame.Frame
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }

// Implicits needed for JSON conversion
import spray.json._

case class RenameFrameArgs(frame: FrameReference,
                           @ArgDoc("""The new name of the frame.""") newName: String) {
  require(frame != null, "frame is required")
  require(newName != null && newName.nonEmpty, "newName is required")
  FrameName.validate(newName)
}

/** Json conversion for arguments and return value case classes */
object RenameFrameJsonFormat {
  import DomainJsonProtocol._
  implicit val RenameFrameArgsFormat = jsonFormat2(RenameFrameArgs)
}

import RenameFrameJsonFormat._
import DomainJsonProtocol._

/**
 * Rename a frame
 */
@PluginDoc(oneLine = "Change the name of the current frame.",
  extended = """Set the name of this frame.""")
class RenameFramePlugin extends CommandPlugin[RenameFrameArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/rename"

  /**
   * Rename a frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RenameFrameArgs)(implicit invocation: Invocation): UnitReturn = {
    val frame: Frame = arguments.frame
    frame.name = arguments.newName
    frame
  }
}
