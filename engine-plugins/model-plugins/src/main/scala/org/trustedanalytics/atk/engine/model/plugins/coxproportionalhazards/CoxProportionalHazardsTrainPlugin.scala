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

package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards

import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards.HazardFunction.HazardFunctionBetaEstimator
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc, SparkCommandPlugin }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import CoxProportionalHazardJSonFormat._

@PluginDoc(oneLine = "Build Cox proportional hazard model.",
  extended = "Fitting a CoxProportionalHazard Model using the covariate column(s)",
  returns = "Trained Cox proportional hazard model")
class CoxProportionalHazardsTrainPlugin extends SparkCommandPlugin[CoxProportionalHazardTrainArgs, CoxProportionalHazardTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:cox_proportional_hazard/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: CoxProportionalHazardTrainArgs)(implicit invocation: Invocation) = 9

  /**
   * Fits Cox hazard function and creates a model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: CoxProportionalHazardTrainArgs)(implicit invocation: Invocation): CoxProportionalHazardTrainReturn = {

    val frame: SparkFrame = arguments.frame
    val schema = frame.schema
    val timeCol = arguments.timeColumn
    val covariateCol = arguments.covariateColumn
    val censoredCol = arguments.censoredColumn
    val convergenceEps = arguments.epsilon
    val maxSteps = arguments.maxSteps
    val initialBeta = arguments.beta

    val sortedRdd = CoxProportionalHazardTrainFunctions.frameToSortedTupleRdd(frame.rdd, timeCol, covariateCol, censoredCol)
    val (beta, error) = HazardFunctionBetaEstimator.newtonRaphson(sortedRdd, convergenceEps, maxSteps, initialBeta)

    //TODO: save beta to the model for predictions. Requires further discussion on multiple beta (covariate variables)...
    val model: Model = arguments.model

    CoxProportionalHazardTrainReturn(beta, error)
  }

}

