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
package org.trustedanalytics.atk.engine.example.plugins

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, SparkCommandPlugin, PluginDoc }

//Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import spray.json._

/* To make msg a mandatory parameter, change to msg: String
   The ArgDoc annotation automatically generates Python docs for the input argument
*/
case class HelloWorldPluginInput(@ArgDoc("""Handle to the frame to be used.""") frame: FrameReference,
                                 @ArgDoc("""Optional message to display.""") msg: Option[String] = None)

case class HelloWorldPluginOutput(value: String)

/** Json conversion for arguments and return value case classes */
object HelloWorldPluginFormat {
  import DomainJsonProtocol._
  implicit val helloWorldInputFormat = jsonFormat2(HelloWorldPluginInput)
  implicit val helloWorldOutputFormat = jsonFormat1(HelloWorldPluginOutput)
}

import HelloWorldPluginFormat._

/* HelloWorldPlugin will embed helloworld command to a frame object.
   Users will be able to access the helloworld function on a python frame object as:
        frame.helloworld()
        frame.helloworld("that was simple!")
   The PluginDoc annotation automatically generates Python documentation for the plugin.
 */
@PluginDoc(oneLine = "This is a Hello World Plugin for Frame.",
  extended =
    """
        Extended Summary
        ----------------
        Extended Summary for Plugin goes here ...
    """,
  returns =
    """
        string
        String that echoes input message
    """)
class HelloWorldPlugin
    extends SparkCommandPlugin[HelloWorldPluginInput, HelloWorldPluginOutput] {

  /**
   * The name of the command, e.g. helloworld
   * For hierarchical commands, you may use the name as xyz/helloworld; the name in that case would be frame:xyz/helloworld
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * For example - you may access these through frame.helloworld (former) or frame.xyz.helloworld (latter)
   */
  override def name: String = "frame:/helloworld"

  override def execute(arguments: HelloWorldPluginInput)(implicit invocation: Invocation): HelloWorldPluginOutput = {
    val inputMessage = arguments.msg.getOrElse("Hello World!")
    HelloWorldPluginOutput(s"Frame says $inputMessage")
  }
}

