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

package org.trustedanalytics.atk.engine.daal.plugins.covariance

import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.frame.{ FrameReference, FrameEntity, FrameName }
import org.trustedanalytics.atk.domain.schema.DataTypes.vector
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Schema }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import com.intel.daal.algorithms.covariance.ResultId

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import DaalCovarianceMatrixJsonFormat._

/**
 * Calculate covariance matrix for the specified columns
 */
@PluginDoc(oneLine = "Calculate covariance matrix for two or more columns.",
  extended = """
Uses Intel Data Analytics and Acceleration Library (DAAL) to compute covariance matrix.

Notes
-----
This function applies only to columns containing numerical data.""",
  returns = "A matrix with the covariance values for the columns.")
class DaalCovarianceMatrixPlugin extends SparkCommandPlugin[DaalCovarianceMatrixArgs, FrameReference] {

  /**
   * The name of the command
   */
  override def name: String = "frame/daal_covariance_matrix"

  /**
   * Calculate covariance matrix for the specified columns
   *
   * @param invocation information about the user and the circumstances at the time of the call, as well as a function
   *                   that can be called to produce a SparkContext that can be used during this invocation
   * @param arguments input specification for covariance matrix
   * @return value of type declared as the Return type
   */
  override def execute(arguments: DaalCovarianceMatrixArgs)(implicit invocation: Invocation): FrameReference = {

    val frame: SparkFrame = arguments.frame
    val dataColumnNames = arguments.dataColumnNames
    frame.schema.requireColumnsAreVectorizable(dataColumnNames)

    // compute covariance
    val outputColumnDataType = frame.schema.columnDataType(dataColumnNames.head)
    val outputVectorLength: Option[Long] = outputColumnDataType match {
      case vector(length) => Some(length)
      case _ => None
    }

    val covarianceRdd = DaalCovarianceFunctions.covarianceMatrix(frame.rdd, dataColumnNames,
      ResultId.covariance, outputVectorLength)
    val outputSchema = Schema.create(arguments.dataColumnNames, DataTypes.float64, outputVectorLength)

    engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by DAAL covariance matrix command"))) {
      newFrame: FrameEntity =>
        if (arguments.matrixName.isDefined) {
          engine.frames.renameFrame(newFrame, FrameName.validate(arguments.matrixName.get))
        }
        newFrame.save(new FrameRdd(outputSchema, covarianceRdd))
    }
  }

}
