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

import org.apache.spark.mllib.recommendation.{ MatrixFactorizationModel, Rating }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering.CollaborativeFilteringJsonFormat._
import org.trustedanalytics.atk.engine.plugin._

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Collaborative filtering score
 */
@PluginDoc(oneLine = "Collaborative Filtering Predict (ALS).",
  extended = """See :ref:`Collaborative Filtering Train
<python_api/models/model-collaborative_filtering/train>` for more information.""",
  returns = """Returns an array of recommendations (as array of csv-strings)""")
class CollaborativeFilteringRecommendPlugin
    extends SparkCommandPlugin[CollaborativeFilteringRecommendArgs, CollaborativeFilteringRecommendReturn] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/recommend"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: CollaborativeFilteringRecommendArgs)(implicit invocation: Invocation): CollaborativeFilteringRecommendReturn = {

    val model: Model = arguments.model
    val data = model.data.convertTo[CollaborativeFilteringData]
    val frames = engine.frames

    val userFrame = frames.loadFrameData(sc, data.userFrame)
    val productFrame = frames.loadFrameData(sc, data.productFrame)
    val alsModel = new MatrixFactorizationModel(data.rank,
      CollaborativeFilteringHelper.toAlsRdd(userFrame, data),
      CollaborativeFilteringHelper.toAlsRdd(productFrame, data))

    CollaborativeFilteringRecommendReturn(formatReturn(recommend(alsModel, arguments)))
  }

  private def recommend(alsModel: MatrixFactorizationModel,
                        arguments: CollaborativeFilteringRecommendArgs): Array[Rating] = {
    val entityId = arguments.entityId
    val numberOfRecommendations = arguments.numberOfRecommendations

    if (arguments.recommendProducts) {
      alsModel.recommendProducts(entityId, numberOfRecommendations)
    }
    else {
      alsModel.recommendUsers(entityId, numberOfRecommendations)
    }
  }

  private def formatReturn(alsRecommendations: Array[Rating]): List[CollaborativeFilteringSingleRecommendReturn] = {
    val recommendationAsList =
      for {
        recommendation <- alsRecommendations
        entityId = recommendation.user.asInstanceOf[Int]
        recommendationId = recommendation.product.asInstanceOf[Int]
        rating = recommendation.rating.asInstanceOf[Double]
      } yield CollaborativeFilteringSingleRecommendReturn(entityId, recommendationId, rating)

    recommendationAsList.toList
  }
}
