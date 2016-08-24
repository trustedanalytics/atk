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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.GenericNewModelArgs
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, CommandPlugin }
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol._
/**
 * Create a 'new' instance of a Linear Regression model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a Lasso Model.",
  extended = """lasso (least absolute shrinkage and selection operator) (also Lasso or LASSO)[1]_ is a regression
analysis method that performs both variable selection and regularization in order to enhance the prediction accuracy
and interpretability of the statistical model it produces.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Lasso
             """,
  returns = """A new instance of a LassoModel""")
class LassoNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:lasso/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:lasso")))
  }
}