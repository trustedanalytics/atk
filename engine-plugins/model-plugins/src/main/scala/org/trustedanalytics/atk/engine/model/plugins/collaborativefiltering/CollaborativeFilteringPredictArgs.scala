/**
 *  Copyright (c) 2016 Intel Corporation 
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

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

/**
 * Arguments to the collaborative filtering predict plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringPredictArgs(model: ModelReference,
                                             frame: FrameReference,
                                             @ArgDoc("""source column name.""") inputSourceColumnName: String,
                                             @ArgDoc("""destination column name.""") inputDestColumnName: String,
                                             @ArgDoc("""A user column name for the output frame""") outputUserColumnName: String = "user",
                                             @ArgDoc("""A product  column name for the output frame""") outputProductColumnName: String = "product",
                                             @ArgDoc("""A rating column name for the output frame""") outputRatingColumnName: String = "rating") {

  require(model != null, "model is required")
  require(frame != null, "batch data as a frame is required")
}