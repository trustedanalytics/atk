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

import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Arguments to the collaborative filtering recommend plugin - see user docs for more on the parameters
 */
case class CollaborativeFilteringRecommendArgs(model: ModelReference,
                                               @ArgDoc("""A user/product id""") entityId: Int,
                                               @ArgDoc("""Number of recommendations""") numberOfRecommendations: Int = 1,
                                               @ArgDoc("""True - products for user; false - users for the product""") recommendProducts: Boolean = true) {

  require(model != null, "model is required")
}

/**
 * This is the return case class for the recommender.
 */
case class CollaborativeFilteringRecommendReturn(value: List[CollaborativeFilteringSingleRecommendReturn])

/**
 * This is the return case class for single recommendation. Each list consists of 3 values (entityId, recommendationId, rating)
 */
case class CollaborativeFilteringSingleRecommendReturn(user: Int, product: Int, rating: Double)

