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

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Creates a 'new' instance of Cox proportional hazard model
 */
@PluginDoc(oneLine = "Creates a 'new' instance of Cox proportional hazard model.",
  extended =
    """A Cox model is a popular statistics technique used in survival analysis to assess the importance of various covariates
      | in the survival times of individuals or objects. The model estimates the survival through a hazard function.
      | The model must be fitted and the final model from a Cox regression analysis will yield an equation for the
      | hazard as a function of several explanatory variables.
      | Interpreting the Cox model involves examining the coefficients for each explanatory variable. A positive
      | regression coefficient for an explanatory variable means that the hazard is higher, and thus the prognosis
      | worse. Conversely, a negative regression coefficient implies a better prognosis for patients with higher values
      | of that variable.The model is described in more detail in the following papers:
      |
      | http://sfb649.wiwi.hu-berlin.de/fedc_homepage/xplore/tutorials/xaghtmlnode28.html
      |https://courses.nus.edu.sg/course/stacar/internet/st3242/handouts/notes3.pdf """.stripMargin)
class CoxProportionalHazardsNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:cox_proportional_hazard/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:cox_proportional_hazard")))
  }
}
