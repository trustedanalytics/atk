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

/* A sample plugin to run spark word count on all columns of a frame */
/* which are strings and report the top 10 */

package org.trustedanalytics.atk.engine.example.plugins

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.SparkContextFactory
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

/* Input arguments for the plugin
   The ArgDoc annotation automatically generates Python docs for the input argument
*/
case class SparkWordCountInput(@ArgDoc("""Handle to the frame to be used.""") frame: FrameReference)

case class SparkWordCountOutput(value: Array[(String, Int)])

/** Json conversion for arguments and return value case classes */
object SparkWordCountFormat {
  implicit val inputFormat = jsonFormat1(SparkWordCountInput)
  implicit val outputFormat = jsonFormat1(SparkWordCountOutput)
}
import SparkWordCountFormat._

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
        dictionary
        Dictionary of top 10 words
    """)
class SparkWordCountPlugin
    extends SparkCommandPlugin[SparkWordCountInput, SparkWordCountOutput] {

  /**
   * The name of the command, e.g. wordcount
   * For hierarchial commands, you may use the name as xyz/wordcount; the name in that case would be frame:xyz/wordcount
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * For example - you may access these through frame.wordcount (former) or frame.xyz.wordcount (latter)
   */
  override def name: String = "frame:/wordcount"

  override def execute(arguments: SparkWordCountInput)(implicit invocation: Invocation): SparkWordCountOutput = {

    /* Load the Frame from the frame id */
    val frame: SparkFrame = arguments.frame

    /* Now you have a schema rdd at your disposal */
    /* Drop all but the string columns before computing word counts */
    val columnNames = frame.schema.columns.filter(column => column.dataType == DataTypes.str).map(_.name)

    val wordRdd = frame.rdd.mapRows(row => {
      /* Now you have a  rdd at your disposal */
      /* Let's filter all columns which are not strings and drop them first before computing the word counts */
      row.values(columnNames)

    }).flatMap(rowValues => {
      /* Extract words from the row */
      rowValues.mkString(" ").split(' ').map(word => (word, 1))
    })

    /* Now let's do a word count on the data and find the top 10 words */
    val top10Words = wordRdd.reduceByKey(_ + _).top(10)(Ordering.by(_._2))

    SparkWordCountOutput(top10Words)
  }
}

