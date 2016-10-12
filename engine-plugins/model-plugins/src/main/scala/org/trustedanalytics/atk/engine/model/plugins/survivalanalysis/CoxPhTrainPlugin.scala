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

package org.trustedanalytics.atk.engine.model.plugins.survivalanalysis

import org.apache.spark.ml.regression.CoxPh
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.ml.atk.plugins.MLJsonProtocol._

@PluginDoc(oneLine = "Build Cox proportional hazard model.",
  extended = "Fitting a CoxProportionalHazard Model using the covariate column(s)",
  returns = "Trained Cox proportional hazard model")
class CoxPhTrainPlugin extends SparkCommandPlugin[CoxPhTrainArgs, CoxPhTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:cox_ph/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Fits Cox hazard function and creates a model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CoxPhTrainArgs)(implicit invocation: Invocation): CoxPhTrainReturn = {

    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val trainFrameRdd = frame.rdd
    val dataFrame = trainFrameRdd.toCoxDataFrame(arguments.covariateColumns, arguments.timeColumn, arguments.censorColumn)

    val cox = CoxPhTrainPlugin.initializeCoxModel(arguments)
    val coxModel = cox.fit(dataFrame)
    val jsonModel = new CoxPhData(coxModel, arguments.covariateColumns, arguments.timeColumn, arguments.censorColumn)
    model.data = jsonModel.toJson.asJsObject

    new CoxPhTrainReturn(coxModel.beta.toArray.toList, coxModel.meanVector.toArray.toList)
  }

}

object CoxPhTrainPlugin {
  /**
   * Initializing the Cox model given the train arguments
   * @param arguments Arguments passed for training the LinearRegression model
   * @return Initialized Cox model with training arguments
   */
  def initializeCoxModel(arguments: CoxPhTrainArgs): CoxPh = {
    val cox = new CoxPh()
    cox.setLabelCol("time")
    cox.setFeaturesCol("features")
    cox.setCensorCol("censor")
    cox.setMaxIter(arguments.maxSteps)
    cox.setTol(arguments.convergenceTolerance)
  }
}