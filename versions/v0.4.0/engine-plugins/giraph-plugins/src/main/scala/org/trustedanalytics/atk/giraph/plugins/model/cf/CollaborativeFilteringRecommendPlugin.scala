/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.giraph.plugins.model.cf

import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.frame.{ SparkFrame, VectorFunctions }
import org.trustedanalytics.atk.giraph.config.cf.{ CollaborativeFilteringData, CollaborativeFilteringJsonFormat, CollaborativeFilteringRecommendArgs }
import org.trustedanalytics.atk.domain.CreateEntityArgs
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, _ }

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

import CollaborativeFilteringJsonFormat._

/**
 * Collaborative filtering recommend model
 */
@PluginDoc(oneLine = "Collaborative filtering (als/cgd) model",
  extended = "see collaborative filtering train for more information",
  returns = "see collaborative filtering train for more information")
class CollaborativeFilteringRecommendPlugin
    extends SparkCommandPlugin[CollaborativeFilteringRecommendArgs, FrameReference] {

  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:collaborative_filtering/recommend"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  override def execute(arguments: CollaborativeFilteringRecommendArgs)(implicit invocation: Invocation): FrameReference = {

    val frames = engine.frames
    val data = arguments.model.data.convertTo[CollaborativeFilteringData]

    val userFrame: SparkFrame = data.userFrameReference
    userFrame.schema.requireColumnIsType(data.userColumnName, DataTypes.string)
    userFrame.schema.requireColumnIsType(data.factorsColumnName, DataTypes.vector(data.numFactors))

    val itemFrame: SparkFrame = data.itemFrameReference
    itemFrame.schema.requireColumnIsType(data.itemColumnName, DataTypes.string)
    itemFrame.schema.requireColumnIsType(data.factorsColumnName, DataTypes.vector(data.numFactors))

    val userFactors = filter(convertFrameRddToNative(userFrame), arguments.name)
    val dotProductRdd = dotProduct(convertFrameRddToNative(itemFrame), userFactors)
    val topK = topKRecommendation(dotProductRdd, arguments.topK)

    val rdd = sc.parallelize(topK)

    val newSchema = FrameSchema(List(
      Column(data.itemColumnName, DataTypes.string),
      Column("recommended_score", DataTypes.float64)
    ))

    val frameRdd = FrameRdd.toFrameRdd(newSchema, rdd.map { case (k, v) => Array(k, v) })

    frames.tryNewFrame(CreateEntityArgs(name = None, description = Some("Recommended results"))) {
      newFrame => newFrame.save(frameRdd)
    }
  }

  def convertFrameRddToNative(frame: SparkFrame): RDD[(String, Seq[Double])] = {
    frame.rdd.mapRows(_.valuesAsArray()).map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Seq[Double]]))
  }

  def topKRecommendation(recommendation: RDD[(String, Double)], topK: Int): Array[(String, Double)] = {

    recommendation.map(_.swap).top(topK)
      .map { case (cnt, data) => (data, cnt) }
  }

  def filter(rdd: RDD[(String, Seq[Double])], name: String): Seq[Double] =
    {
      val filteredRdd = rdd.filter(_._1 == name)
      if (filteredRdd.isEmpty()) throw new RuntimeException("No entry found for " + name) else filteredRdd.first._2
    }

  def dotProduct(rdd: RDD[(String, Seq[Double])], factors: Seq[Double]): RDD[(String, Double)] = {
    rdd.map(row => {
      (row._1, VectorFunctions.dotProduct(factors, row._2))
    })
  }
}
