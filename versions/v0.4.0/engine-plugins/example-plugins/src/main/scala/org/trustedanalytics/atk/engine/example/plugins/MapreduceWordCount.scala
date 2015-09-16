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
/* Here we demonstrate a sample plugin which reads data from HDFS and runs a mapreduce job on the file to compute
   the number of words in the file and write back the counts to another file. The input file is being read as plain
   text file.

   WordCount.java has been used from Hadoop example source code
*/

/* A sample plugin to run spark word count on all columns of a frame */
/* which are strings and report the top 10 */

package org.trustedanalytics.atk.engine.example.plugins

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.example.plugins.wc.WordCount
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

/* To make msg a mandatory parameter, change to msg: String
   The ArgDoc annotation automatically generates Python docs for the input argument
*/
case class MapreduceWordCountInput(@ArgDoc("""Handle to the frame to be used.""") frame: FrameReference,
                                   @ArgDoc("""Path to input directory.""") inputDir: String,
                                   @ArgDoc("""Path to output directory.""") outputDir: String)

case class MapreduceWordCountOutput(value: String)

/** Json conversion for arguments and return value case classes */
object MapreduceWordCountFormat {
  implicit val inputFormat = jsonFormat3(MapreduceWordCountInput)
  implicit val outputFormat = jsonFormat1(MapreduceWordCountOutput)
}
import MapreduceWordCountFormat._

/* WordCountPlugin will embed wordcount command to a frame object.
   Users will be able to access the wordcount function on a python frame object as:
        frame.wordcount()
 */

@PluginDoc(oneLine = "Counts and reports the top 10 words across all columns with string data in a frame.",
  extended =
    """
        Extended Summary
        ----------------
        Extended Summary for Plugin goes here ...
    """,
  returns =
    """
        string
        String that echos input message
    """)
class MapreduceWordCountPlugin
    extends CommandPlugin[MapreduceWordCountInput, MapreduceWordCountOutput] {

  /**
   * The name of the command, e.g. wordcount
   * For hierarchial commands, you may use the name as xyz/wordcount; the name in that case would be frame:xyz/wordcount
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * For example - you may access these through frame.wordcount (former) or frame.xyz.wordcount (latter)
   */
  override def name: String = "frame:/mapreducewordcount"

  override def execute(arguments: MapreduceWordCountInput)(implicit invocation: Invocation): MapreduceWordCountOutput = {

    val hdfsWorkingDir = EngineConfig.fsRoot

    println(s"HDFS Working Dir: $hdfsWorkingDir")

    val inputParams = Array(s"$hdfsWorkingDir/${arguments.inputDir}", s"$hdfsWorkingDir/${arguments.outputDir}")

    /* Invoke Hadoop Mapreduce word count on the input directory and write back the results to output directory */
    WordCount.run(inputParams)

    MapreduceWordCountOutput("MapReduce job is complete")
  }
}

