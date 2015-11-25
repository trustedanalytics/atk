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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.DoubleValue
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import CollaborativeFilteringJsonFormat._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Collaborative filtering score
 */
@PluginDoc(oneLine = "Collaborative Filtering Predict (ALS).",
  extended = """See :ref:`Collaborative Filtering Train
<python_api/models/model-collaborative_filtering/train>` for more information.""",
  returns = """Returns a double representing the probability if the user(i) to like product (j)""")
class CollaborativeFilteringScorePlugin
    extends SparkCommandPlugin[CollaborativeFilteringScoreArgs, DoubleValue] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/score"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: CollaborativeFilteringScoreArgs)(implicit invocation: Invocation): DoubleValue = {

    val model: Model = arguments.model
    val data = model.data.convertTo[CollaborativeFilteringData]
    val frames = engine.frames

    val userFrame = frames.loadFrameData(sc, data.userFrame)
    val productFrame = frames.loadFrameData(sc, data.productFrame)
    val alsModel = new MatrixFactorizationModel(data.rank, toAlsRdd(userFrame, data), toAlsRdd(productFrame, data))

    DoubleValue(alsModel.predict(arguments.userId, arguments.itemId))
  }

  private def toAlsRdd(userFrame: FrameRdd, data: CollaborativeFilteringData): RDD[(Int, Array[Double])] = {
    val idColName = data.userFrame.schema.column(0).name
    val featuresColName = data.userFrame.schema.column(1).name

    userFrame.mapRows(row => (row.intValue(idColName),
      DataTypes.vector.parse(row.value(featuresColName)).get.toArray))
  }
}
