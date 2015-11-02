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


package org.trustedanalytics.atk.giraph.config.cf

import org.apache.commons.lang3.StringUtils
import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.plugin.ArgDoc

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * Arguments to the CfRecommend plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringRecommendArgs(model: ModelReference,
                                               @ArgDoc("""An entity name from the first column of the input frame""") name: String,
                                               @ArgDoc("""positive integer representing the top recommendations for the name""") topK: Int) {

  require(StringUtils.isNotBlank(name), "entity name is required")
  require(topK > 0, "top k must be greater than 0")
  require(model != null, "model is required")
}

/** Json conversion for arguments and return value case classes */
object CollaborativeFilteringRecommendJsonFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._

  implicit val cfRecommendFormat = jsonFormat3(CollaborativeFilteringRecommendArgs)
}
