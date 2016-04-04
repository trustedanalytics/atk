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

package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
/**
 * Create a 'new' instance of a DAAL k-means model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a DAAL k-means model.",
  extended = """k-means [1]_ is an unsupervised algorithm used to partition
the data into 'k' clusters.
Each observation can belong to only one cluster, the cluster with the nearest
mean.
The k-means model is initialized, trained on columns of a frame, and used to
predict cluster assignments for a frame.

This model runs the DAAL implementation of k-means[2]_. The K-Means clustering
algorithm computes centroids using the Lloyd method[3]_

.. rubric:: footnotes

.. [1] https://en.wikipedia.org/wiki/K-means_clustering
.. [2] https://software.intel.com/en-us/daal
.. [3] https://en.wikipedia.org/wiki/Lloyd%27s_algorithm""",
  returns = """A new instance of DaalKMeansModel""")
class DaalKMeansNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {

  override def name: String = "model:daal_k_means/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:daal_k_means")))
  }
}
