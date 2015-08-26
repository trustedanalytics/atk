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

package org.trustedanalytics.atk.engine.frame.plugins.sortedk

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
object SortedKJsonFormat {
  implicit val sortedKFormat = jsonFormat4(SortedKArgs)
}

import SortedKJsonFormat._

/**
 * Plugin that returns the top-K rows in a frame ordered by column(s).
 */

@PluginDoc(oneLine = "Get a sorted subset of the data.",
  extended = """Take the first k (sorted) rows for the currently active Frame.
Rows are sorted by column values in either ascending or descending order.

Returning the first k (sorted) rows is more efficient than sorting the
entire frame when k is much smaller than the number of rows in the frame.

Notes
-----
The number of sorted rows (k) should be much smaller than the number of rows
in the original frame.

In particular:

1) The number of sorted rows (k) returned should fit in Spark driver memory.
  The maximum size of serialized results that can fit in the Spark driver is
  set by the Spark configuration parameter *spark.driver.maxResultSize*.

2) If you encounter a Kryo buffer overflow exception, increase the Spark
  configuration parameter *spark.kryoserializer.buffer.max.mb*.

3) Use Frame.sort() instead if the number of sorted rows (k) is
  very large (i.e., cannot fit in Spark driver memory).""",
  returns = "A new frame with the first k sorted rows from the original frame.")
class SortedKPlugin extends SparkCommandPlugin[SortedKArgs, UnitReturn] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/sorted_k"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: SortedKArgs)(implicit invocation: Invocation) = 2

  /**
   * Plugin that returns the top-K rows in a frame ordered by column(s).
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return New frame with top-K sorted rows.
   */
  override def execute(arguments: SortedKArgs)(implicit invocation: Invocation): UnitReturn = {
    // load frame
    val frame: SparkFrame = arguments.frame

    // return new frame with top-k sorted records
    val sortedKFrame = SortedKFunctions.takeOrdered(
      frame.rdd,
      arguments.k,
      arguments.columnNamesAndAscending,
      arguments.reduceTreeDepth
    )

    // save the new frame
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by sorted_k command"))) {
      newFrame => newFrame.save(sortedKFrame)
    }
  }
}
