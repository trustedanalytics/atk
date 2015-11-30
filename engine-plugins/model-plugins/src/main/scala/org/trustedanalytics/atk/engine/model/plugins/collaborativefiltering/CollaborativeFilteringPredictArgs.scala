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

package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Arguments to the collaborative filtering predict plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringPredictArgs(model: ModelReference,
                                             graph: GraphReference,
                                             @ArgDoc("""A user column name for the output frame""") userColumnName: String = "user",
                                             @ArgDoc("""A product  column name for the output frame""") productColumnName: String = "product",
                                             @ArgDoc("""A rating column name for the output frame""") ratingColumnName: String = "rating") {

  require(model != null, "model is required")
  require(graph != null, "batch data as a graph is required")
}
