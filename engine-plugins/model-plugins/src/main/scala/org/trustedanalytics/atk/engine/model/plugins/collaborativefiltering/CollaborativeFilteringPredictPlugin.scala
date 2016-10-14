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
import org.apache.spark.mllib.recommendation.{ Rating, MatrixFactorizationModel }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.{ CreateEntityArgs }
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, SparkFrameStorage }
import org.trustedanalytics.atk.engine.graph.SparkGraph
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering.CollaborativeFilteringJsonFormat._
import org.trustedanalytics.atk.engine.plugin._

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/**
 * Collaborative filtering predict
 *
 */
@PluginDoc(oneLine = "Collaborative Filtering Predict (ALS).",
  extended = """See :ref:`Collaborative Filtering Train
<python_api/models/model-collaborative_filtering/train>` for more information.""",
  returns = """Returns a double representing the probability if the user(i) to like product (j)""")
class CollaborativeFilteringPredictPlugin
    extends SparkCommandPlugin[CollaborativeFilteringPredictArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/predict"

  override def execute(arguments: CollaborativeFilteringPredictArgs)(implicit invocation: Invocation): FrameReference = {
    val frames = engine.frames
    val edgeFrame: SparkFrame = arguments.frame
    val schema = edgeFrame.schema
    val model: Model = arguments.model
    val data = model.data.convertTo[CollaborativeFilteringData]

    schema.requireColumnIsType(arguments.inputSourceColumnName, DataTypes.int)
    schema.requireColumnIsType(arguments.inputDestColumnName, DataTypes.int)
    require(edgeFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    val alsPredictInput = edgeFrame.rdd.map(edge => {
      (edge.getInt(schema.columnIndex(arguments.inputSourceColumnName)),
        edge.getInt(schema.columnIndex(arguments.inputDestColumnName))
      )
    })

    val userFrame = frames.loadFrameData(sc, data.userFrame)
    val productFrame = frames.loadFrameData(sc, data.productFrame)
    val alsModel = new MatrixFactorizationModel(data.rank,
      CollaborativeFilteringHelper.toAlsRdd(userFrame, data),
      CollaborativeFilteringHelper.toAlsRdd(productFrame, data))

    frames.tryNewFrame(CreateEntityArgs(description = Some("created by ALS publish operation"))) {
      frame: FrameEntity =>
        frame.save(toFrameRdd(alsModel.predict(alsPredictInput), arguments))
    }
  }

  private def toFrameRdd(modelRdd: RDD[Rating],
                         arguments: CollaborativeFilteringPredictArgs)(implicit invocation: Invocation): FrameRdd = {
    val schema = FrameSchema(List(
      Column(arguments.outputUserColumnName, DataTypes.int),
      Column(arguments.outputProductColumnName, DataTypes.int),
      Column(arguments.outputRatingColumnName, DataTypes.float32)))
    val rowRdd = modelRdd.map {
      case alsRating => Row(alsRating.user, alsRating.product, DataTypes.toFloat(alsRating.rating))
    }
    new FrameRdd(schema, rowRdd)
  }

}
