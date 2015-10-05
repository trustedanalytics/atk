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
package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, Invocation }
import org.trustedanalytics.atk.engine.plugin.CommandPlugin
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._
/**
 * Create a 'new' instance of a NaiveBayes model
 */

@PluginDoc(oneLine = "Create a 'new' instance of a Naive Bayes model",
  extended = """Naive Bayes [1]_ is a probabilistic classifier with strong
independence assumptions between features.
It computes the conditional probability distribution of each feature given label,
and then applies Bayes' theorem to compute the conditional probability
distribution of a label given an observation, and use it for prediction.
The Naive Bayes model is initialized, trained on columns of a frame, and used
to predict the value of the dependent variable given the independent
observations of a frame.
This model runs the MLLib implementation of Naive Bayes [2]_.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Naive_Bayes_classifier
.. [2] https://spark.apache.org/docs/1.3.0/mllib-naive-bayes.html
             """,
  returns = """A new instance of NaiveBayesModel""")
class NaiveBayesNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:naive_bayes/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:naive_bayes")))
  }
}
