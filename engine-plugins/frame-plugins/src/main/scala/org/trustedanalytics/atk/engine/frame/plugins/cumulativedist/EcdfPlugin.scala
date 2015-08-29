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

package org.trustedanalytics.atk.engine.frame.plugins.cumulativedist

import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes, Column }
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin

import org.trustedanalytics.atk.domain.{ DomainJsonProtocol, CreateEntityArgs }
import org.apache.spark.frame.FrameRdd

// Implicits needed for JSON conversion
import spray.json._

case class EcdfArgs(frame: FrameReference,
                    @ArgDoc("The name of the input column containing sample.") column: String,
                    @ArgDoc("A name for the resulting frame which is created by this operation.") resultFrameName: Option[String] = None) {
  require(frame != null, "frame is required")
  require(column != null, "column is required")

  def getResultFrameName: Option[String] = {
    resultFrameName match {
      case Some(n) =>
        FrameName.validate(n)
        Some(n)
      case _ => None
    }
  }

}

/** Json conversion for arguments and return value case classes */
object EcdfJsonFormat {
  import DomainJsonProtocol._
  implicit val EcdfArgsFormat = jsonFormat3(EcdfArgs)
}

import EcdfJsonFormat._
import DomainJsonProtocol._
/**
 * Empirical Cumulative Distribution for a column
 */
@PluginDoc(oneLine = "Builds new frame with columns for data and distribution.",
  extended = """Generates the :term:`empirical cumulative distribution` for the input column.""",
  returns = "A new Frame containing each distinct value in the sample and its corresponding ECDF value.")
class EcdfPlugin extends SparkCommandPlugin[EcdfArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "frame/ecdf"

  override def numberOfJobs(arguments: EcdfArgs)(implicit invocation: Invocation) = 6

  /**
   * Empirical Cumulative Distribution for a column
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: EcdfArgs)(implicit invocation: Invocation): FrameReference = {
    // validate arguments
    val frame: SparkFrame = arguments.frame
    val sampleColumn = frame.schema.column(arguments.column)
    require(sampleColumn.dataType.isNumerical, s"Invalid column ${sampleColumn.name} for ECDF.  Expected a numeric data type, but got ${sampleColumn.dataType}.")
    val ecdfSchema = FrameSchema(List(sampleColumn.copy(), Column(sampleColumn.name + "_ECDF", DataTypes.float64)))

    // run the operation
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by ECDF operation"))) { ecdfFrame: FrameEntity =>
      if (arguments.resultFrameName.isDefined) {
        engine.frames.renameFrame(ecdfFrame, FrameName.validate(arguments.resultFrameName.get))
      }
      val ecdfRdd = CumulativeDistFunctions.ecdf(frame.rdd, sampleColumn)
      ecdfFrame.save(new FrameRdd(ecdfSchema, ecdfRdd))
    }
  }
}
