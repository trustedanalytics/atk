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

package org.trustedanalytics.atk.engine.frame.plugins

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, PythonRddStorage }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Copies specified columns into a new Frame object, optionally renaming them and/or filtering them
 */
@PluginDoc(oneLine = "New frame with copied columns.",
  extended = """Copies specified columns into a new Frame object, optionally
renaming them and/or filtering them.""",
  returns = "New Frame object.")
class CopyFramePlugin extends SparkCommandPlugin[CopyFrameArgs, FrameReference] {

  override def name: String = "frame/copy"

  override def numberOfJobs(arguments: CopyFrameArgs)(implicit invocation: Invocation) = {
    arguments.where match {
      case Some(function) => 2 // predicated copy requires a row count operation
      case None => 1
    }
  }

  /**
   * Create a copy of frame with options: select only certain columns, rename columns, condition which rows are copied
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CopyFrameArgs)(implicit invocation: Invocation): FrameReference = {

    val frame: SparkFrame = arguments.frame

    val finalRdd = if (arguments.where.isDefined) {
      val finalSchema = arguments.columns.isDefined match {
        case true => frame.schema.copySubsetWithRename(arguments.columns.get)
        case false => frame.schema
      }

      // predicated copy - the column select is baked into the 'where' function, see Python client spark.py
      // Note: Update if UDF wrapping logic ever moves out of the client and into the server
      PythonRddStorage.mapWith(frame.rdd, arguments.where.get, finalSchema, sc)
    }
    else {
      if (arguments.columns.isDefined) {
        frame.rdd.selectColumnsWithRename(arguments.columns.get)
      }
      else {
        frame.rdd
      }
    }

    engine.frames.tryNewFrame(CreateEntityArgs(name = arguments.name, description = Some("created by copy command"))) {
      newFrame => newFrame.save(finalRdd)
    }
  }
}
