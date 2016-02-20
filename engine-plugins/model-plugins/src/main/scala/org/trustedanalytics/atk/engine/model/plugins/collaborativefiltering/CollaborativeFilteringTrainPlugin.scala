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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.recommendation.{ MatrixFactorizationModel, ALS, Rating }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.{ StringValue, CreateEntityArgs }
import org.trustedanalytics.atk.domain.frame.{ FrameEntity }
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column, FrameSchema }
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, SparkFrameStorage }
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ SparkCommandPlugin, Invocation, PluginDoc }
import CollaborativeFilteringJsonFormat._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

@PluginDoc(oneLine = "Collaborative filtering (ALS) model",
  extended = "",
  returns = "Execution result summary for ALS collaborative filtering")
class CollaborativeFilteringTrainPlugin
    extends SparkCommandPlugin[CollaborativeFilteringTrainArgs, UnitReturn] {

  /**
   * The name of the command, e.g. frame:/label_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/train"

  override def execute(arguments: CollaborativeFilteringTrainArgs)(implicit invocation: Invocation): UnitReturn = {
    val frames = engine.frames
    val edgeFrame: SparkFrame = arguments.frame
    val schema = edgeFrame.schema

    schema.requireColumnIsType(arguments.sourceColumnName, DataTypes.int)
    schema.requireColumnIsType(arguments.destColumnName, DataTypes.int)
    require(edgeFrame.isParquet, "frame must be stored as parquet file, or support for new input format is needed")

    val alsInput = edgeFrame.rdd.mapRows(row =>
      {
        Rating(row.intValue(arguments.sourceColumnName),
          row.intValue(arguments.destColumnName),
          row.doubleValue(arguments.weightColumnName))
      })

    val als = new ALS()
      .setRank(arguments.numFactors)
      .setIterations(arguments.maxSteps)
      .setRank(arguments.numFactors)
      .setLambda(arguments.regularization)
      .setAlpha(arguments.alpha)
    val alsTrainedModel: MatrixFactorizationModel = als.run(alsInput)

    val model: Model = arguments.model
    val outputSchema = FrameSchema(List(Column("id", DataTypes.int), Column("features", DataTypes.vector(arguments.numFactors))))
    val userFrameEntity = toFrameEntity(frames, outputSchema, alsTrainedModel.userFeatures)
    val productFrameEntity = toFrameEntity(frames, outputSchema, alsTrainedModel.productFeatures)

    model.data = CollaborativeFilteringData(alsTrainedModel.rank,
      userFrameEntity,
      productFrameEntity).toJson.asJsObject

  }

  private def toFrameEntity(frames: SparkFrameStorage,
                            alsRddSchema: FrameSchema,
                            modelRdd: RDD[(Int, Array[Double])])(implicit invocation: Invocation): FrameEntity = {
    val rowRdd = modelRdd.map {
      case (id, features) => Row(id, DataTypes.toVector(features.length)(features))
    }
    frames.tryNewFrame(CreateEntityArgs(description = Some("created by ALS train operation"))) {
      frame: FrameEntity =>
        frame.save(new FrameRdd(alsRddSchema, rowRdd))
    }

  }
}
