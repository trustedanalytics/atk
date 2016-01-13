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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Create a new instance of a gmm model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a gmm model.",
extended =
  """A Gaussian Mixture Model [1]_ represents a distribution where the observations are drawn from one of the k
Gaussian sub-distributions, each with its own probability. Each observation can belong to only one cluster,
the cluster representing the distribution with highest probability for that observation.

The gmm model is initialized, trained on columns of a frame, and used to
predict cluster assignments for a frame.
This model runs the MLLib implementation of gmm [2]_ with enhanced
feature of computing the number of elements in each cluster during training.
During predict, it computes the cluster assignment of the observations given in the frame.

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/Mixture_model#Multivariate_Gaussian_mixture_model
.. [2] https://spark.apache.org/docs/1.5.0/mllib-clustering.html#gaussian-mixture
  """)
class GMMNewPlugin extends SparkCommandPlugin[GenericNewModelArgs, ModelReference] {

  override def name: String = "model:gmm/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:gmm")))
  }
}
