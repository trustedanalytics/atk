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

package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.apache.spark.mllib.clustering.{AtkLdaModel, AtkLdaModel$}
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._
import LdaJsonFormat._

/**
 * Predict plugin for Latent Dirichlet Allocation
 */
@PluginDoc(oneLine = "Predict conditional probabilities of topics given document.",
  extended =
    """Predicts conditional probabilities of topics given document using trained Latent Dirichlet Allocation model.
The input document is represented as a list of strings""",
  returns = """Dictionary containing predicted topics.
The data returned is composed of multiple components\:

|   **list of doubles** | *topics_given_doc*
|       List of conditional probabilities of topics given document.
|   **int** : *new_words_count*
|       Count of new words in test document not present in training set.
|   **double** | *new_words_percentage*
|       Percentage of new words in test document.""")
class LdaPredictPlugin extends CommandPlugin[LdaModelPredictArgs, LdaModelPredictReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:sparklda/predict"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: LdaModelPredictArgs)(implicit invocation: Invocation) = 1

  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: LdaModelPredictArgs)(implicit invocation: Invocation): LdaModelPredictReturn = {
    val model: Model = arguments.model
    val document: List[String] = arguments.document

    val ldaModel = model.data.convertTo[AtkLdaModel]
    ldaModel.predict(document)
  }

}

