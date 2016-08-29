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

package org.trustedanalytics.atk.engine.model.plugins.timeseries

import com.cloudera.sparkts.models.ARIMAX
import org.apache.spark.mllib.linalg.DenseVector
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.scoring.models.ARIMAXData

// Implicits needed for JSON conversion
import org.trustedanalytics.atk.engine.model.plugins.timeseries.ARIMAXJsonProtocol._
import spray.json._

class ARIMAXTrainPlugin extends SparkCommandPlugin[ARIMAXTrainArgs, ARIMAXTrainReturn] {
  /**
   * The name of the command.
   */

  override def name: String = "model:arimax/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   *
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: ARIMAXTrainArgs)(implicit invocation: Invocation) = 15

  /**
   * Run the spark time series ARIMAX fitmodel() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: ARIMAXTrainArgs)(implicit invocation: Invocation): ARIMAXTrainReturn = {
    val frame: SparkFrame = arguments.frame
    val model = arguments.model
    val trainFrameRdd = frame.rdd

    trainFrameRdd.cache()

    val userInitParams = if (arguments.userInitParams.isDefined) arguments.userInitParams.get.toArray else null
    val (yVector, xMatrix) = ARXFunctions.getYandXFromFrame(trainFrameRdd, arguments.timeseriesColumn, arguments.xColumns)
    val yyy = new DenseVector(yVector.toArray)
    val arimaxModel = ARIMAX.fitModel(arguments.p, arguments.d, arguments.q, yyy, xMatrix, arguments.xregMaxLag,
      arguments.includeOriginalXreg, arguments.includeIntercept, userInitParams)

    trainFrameRdd.unpersist()

    val jsonModel = new ARIMAXData(arimaxModel, arguments.xColumns)
    model.data = jsonModel.toJson.asJsObject

    val c = arimaxModel.coefficients(0)
    val ar = arimaxModel.coefficients.slice(1, arguments.p + 1)
    val ma = arimaxModel.coefficients.slice(arguments.p + 1, arguments.p + arguments.q + 1)
    val xreg = arimaxModel.coefficients.drop(arguments.p + arguments.q + 1)

    new ARIMAXTrainReturn(c, ar, ma, xreg)
  }

}
