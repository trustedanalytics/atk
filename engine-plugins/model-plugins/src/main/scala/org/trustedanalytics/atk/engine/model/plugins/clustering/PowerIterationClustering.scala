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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.domain.{ CreateEntityArgs, DomainJsonProtocol }
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ Invocation, SparkCommandPlugin }
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class PowerIterationClusteringArgs(model: ModelReference,
                                        frame: FrameReference,
                                        sourceColumn: String,
                                        destinationColumn: String,
                                        similarity: String,
                                        k: Int = 2,
                                        maxIterations: Int = 100,
                                        initializationMode: String = "random")

case class PowerIterationClusteringReturn(frame: FrameReference, k: Int)

class PowerIterationClusteringPlugin extends SparkCommandPlugin[PowerIterationClusteringArgs, PowerIterationClusteringReturn] {
  override def name: String = "model:power_iteration_clustering/run"

  override def numberOfJobs(arguments: PowerIterationClusteringArgs)(implicit invocation: Invocation) = 1

  override def execute(arguments: PowerIterationClusteringArgs)(implicit invocation: Invocation): PowerIterationClusteringReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    val trainFrameRdd = frame.rdd
    trainFrameRdd.cache()
    val similaritiesRdd = trainFrameRdd.toSourceDestinationDistance(arguments.sourceColumn, arguments.destinationColumn, arguments.similarity)
    val powerIterationClustering = PowerIterationClusteringPlugin.initializePIC(arguments)

    val output = powerIterationClustering.run(similaritiesRdd)
    val assignments = output.assignments
    val rdd: RDD[Array[Any]] = assignments.map(x => Array(x.id, x.cluster))
    val schema = FrameSchema(List(Column("id", DataTypes.int64), Column("cluster", DataTypes.int32)))
    val frameRdd = FrameRdd.toFrameRdd(schema, rdd)
    val frameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by PIC operation"))) { newPredictedFrame: FrameEntity =>
      newPredictedFrame.save(frameRdd)
    }
    val k = output.k
    new PowerIterationClusteringReturn(frameReference, k)

  }
}

object PowerIterationClusteringPlugin {
  def initializePIC(arguments: PowerIterationClusteringArgs): PowerIterationClustering = {
    val pic = new PowerIterationClustering()

    pic.setInitializationMode(arguments.initializationMode)
    pic.setK(arguments.k)
    pic.setMaxIterations(arguments.maxIterations)
  }
}