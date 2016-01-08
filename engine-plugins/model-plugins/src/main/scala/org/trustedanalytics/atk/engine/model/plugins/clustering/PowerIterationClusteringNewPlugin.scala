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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.model.{ GenericNewModelArgs, KMeansNewArgs, ModelReference }
import org.trustedanalytics.atk.engine.plugin.{ CommandPlugin, Invocation, PluginDoc }

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Create a 'new' instance of a power iteration clustering model
 */
@PluginDoc(oneLine = "Create a 'new' instance of a PowerIterationClustering model.",
  extended = """Power Iteration Clustering [1]_ is a scalable and efficient algorithm for clustering vertices of a graph given pairwise similarities as edge properties.
A Power Iteration Clustering model is initialized and the cluster assignments of the vertices can be predicted on specifying the source column, destination column and similarity column of the given frame.

.. rubric:: footnotes

.. [1] http://www.cs.cmu.edu/~wcohen/postscript/icm12010-pic-final.pdf
.. [2] https://spark.apache.org/docs/1.5.0/mllib-clustering.html#power-iteration-clustering-pic""",
  returns = """A new instance of PowerIterationClustering Model""")
class PowerIterationClusteringNewPlugin extends CommandPlugin[GenericNewModelArgs, ModelReference] {

  override def name: String = "model:power_iteration_clustering/new"

  override def execute(arguments: GenericNewModelArgs)(implicit invocation: Invocation): ModelReference = {
    engine.models.createModel(CreateEntityArgs(name = arguments.name, entityType = Some("model:power_iteration_clustering")))
  }
}
