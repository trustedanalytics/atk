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

package org.trustedanalytics.atk.engine.frame.plugins.statistics.covariance

import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Schema }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.{ SparkCommandPlugin, SparkInvocation }
import org.trustedanalytics.atk.domain.schema.FrameSchema
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.CovarianceMatrixArgs
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.DataTypes.vector

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Calculate covariance matrix for the specified columns
 */
@PluginDoc(oneLine = "Calculate covariance matrix for two or more columns.",
  extended = """Notes
-----
This function applies only to columns containing numerical data.""",
  returns = "A matrix with the covariance values for the columns.")
class CovarianceMatrixPlugin extends SparkCommandPlugin[CovarianceMatrixArgs, FrameReference] {

  /**
   * The name of the command
   */
  override def name: String = "frame/covariance_matrix"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CovarianceMatrixArgs)(implicit invocation: Invocation) = 7

  /**
   * Calculate covariance matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance matrix
   * @return value of type declared as the Return type
   */
  override def execute(arguments: CovarianceMatrixArgs)(implicit invocation: Invocation): FrameReference = {

    val frame: SparkFrame = arguments.frame
    frame.schema.requireColumnsAreVectorizable(arguments.dataColumnNames)

    // compute covariance
    val outputColumnDataType = frame.schema.columnDataType(arguments.dataColumnNames.head)
    val outputVectorLength: Option[Long] = outputColumnDataType match {
      case vector(length) => Some(length)
      case _ => None
    }
    val covarianceRdd = CovarianceFunctions.covarianceMatrix(frame.rdd, arguments.dataColumnNames, outputVectorLength)

    val outputSchema = getOutputSchema(arguments.dataColumnNames, outputVectorLength)
    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by covariance matrix command"))) { newFrame: FrameEntity =>
      if (arguments.matrixName.isDefined) {
        engine.frames.renameFrame(newFrame, FrameName.validate(arguments.matrixName.get))
      }
      newFrame.save(new FrameRdd(outputSchema, covarianceRdd))
    }
  }

  // Get output schema for covariance matrix
  private def getOutputSchema(dataColumnNames: List[String], outputVectorLength: Option[Long] = None): FrameSchema = {
    val outputColumns = outputVectorLength match {
      case Some(length) => List(Column(dataColumnNames.head, DataTypes.vector(length)))
      case _ => dataColumnNames.map(name => Column(name, DataTypes.float64))
    }
    FrameSchema(outputColumns)
  }
}
