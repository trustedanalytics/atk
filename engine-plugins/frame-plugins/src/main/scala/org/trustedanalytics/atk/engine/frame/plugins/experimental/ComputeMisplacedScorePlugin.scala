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

package org.trustedanalytics.atk.engine.frame.plugins.experimental

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.schema.DataTypes

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Adds one or more new columns to the frame by evaluating the given func on each row.
 */
@PluginDoc(oneLine = "",
  extended = "",
  returns = "")
class ComputeMisplacedScorePlugin extends SparkCommandPlugin[ComputeMisplacedScoreArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/compute_misplaced_score"

  /**
   * Adds one or more new columns to the frame by evaluating the given func on each row.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ComputeMisplacedScoreArgs)(implicit invocation: Invocation): FrameReference = {
    val frame: SparkFrame = arguments.frame
    val rdd = frame.rdd

    val locationFrame: SparkFrame = arguments.locationFrame
    val locations = for {
      iter <- locationFrame.rdd.mapRows(row => row.valuesAsArray()).collect()
      location = iter(0).asInstanceOf[String]
      x = iter(1).asInstanceOf[Double]
      y = iter(2).asInstanceOf[Double]
    } yield (location, (x, y))
    val bv = sc.broadcast(locations.toMap)

    val newFrameRdd = ComputeMisplacedScoreImpl.computeMisplacedScoreUsingBroadcastJoin(rdd, bv)
    engine.frames.tryNewFrame(CreateEntityArgs(name = Some("misplaced_score_frame"), description = Some("MS"))) {
      newFrame => newFrame.save(newFrameRdd)
    }
  }
}

