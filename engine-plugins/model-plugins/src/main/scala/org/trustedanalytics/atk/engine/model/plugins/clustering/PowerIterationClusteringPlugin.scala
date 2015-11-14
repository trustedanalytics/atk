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
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ PluginDoc, ArgDoc, Invocation, SparkCommandPlugin }
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class PowerIterationClusteringArgs(@ArgDoc("""Handle of the model to be used""") model: ModelReference,
                                        @ArgDoc("""Frame storing the graph to be clustered""") frame: FrameReference,
                                        @ArgDoc("""Name of the column containing the source node""") sourceColumn: String,
                                        @ArgDoc("""Name of the column containing the destination node""") destinationColumn: String,
                                        @ArgDoc("""Name of the column containing the similarity""") similarityColumn: String,
                                        @ArgDoc("""Number of clusters to cluster the graph into. Default is 2""") k: Int = 2,
                                        @ArgDoc("""Maximum number of iterations of the power iteration loop. Default is 100""") maxIterations: Int = 100,
                                        @ArgDoc("""Initialization mode of power iteration clustering. This can be either "random" to use a
random vector as vertex properties, or "degree" to use normalized sum similarities. Default is "random".""") initializationMode: String = "random") {
  require(model != null, "model must not be null")
  require(frame != null, "frame must not be null")
  require(sourceColumn != null && sourceColumn.nonEmpty, "sourceColumn must not be null nor empty")
  require(destinationColumn != null && destinationColumn.nonEmpty, "destinationColumn must not be null nor empty")
  require(similarityColumn != null && similarityColumn.nonEmpty, "similarityColumn must not be null nor empty")
  require(k >= 2, "Number of clusters must be must be greater than 1")
  require(maxIterations >= 1, "Maximum number of iterations must be greater than 0")
}

/**
 * The return object of Power Iteration Clustering's predict operation
 * @param frame The handle to the frame storing the graph as an edge list
 * @param k Number of clusters
 * @param clusterSize Map storing the number of elements in each cluster
 */
case class PowerIterationClusteringReturn(frame: FrameReference, k: Int, clusterSize: Map[String, Int])

@PluginDoc(oneLine = "Predict the clusters to which the nodes belong to",
  extended =
    """Predict the cluster assignments for the nodes of the graph and create a new frame with a column storing node id and a column with corresponding cluster assignment""",
  returns = """A new frame with a column storing node id and a column with corresponding cluster assignment""")
class PowerIterationClusteringPlugin extends SparkCommandPlugin[PowerIterationClusteringArgs, PowerIterationClusteringReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:power_iteration_clustering/predict"

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */

  override def numberOfJobs(arguments: PowerIterationClusteringArgs)(implicit invocation: Invocation) = 9
  /**
   * Get the predictions for observations in a test frame
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: PowerIterationClusteringArgs)(implicit invocation: Invocation): PowerIterationClusteringReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    val trainFrameRdd = frame.rdd
    trainFrameRdd.cache()
    val similaritiesRdd = trainFrameRdd.toSourceDestinationSimilarityRDD(arguments.sourceColumn, arguments.destinationColumn, arguments.similarityColumn)
    val powerIterationClustering = PowerIterationClusteringPlugin.initializePIC(arguments)

    val output = powerIterationClustering.run(similaritiesRdd)
    val assignments = output.assignments
    val rdd: RDD[Array[Any]] = assignments.map(x => Array(x.id, x.cluster + 1))
    val schema = FrameSchema(List(Column("id", DataTypes.int64), Column("cluster", DataTypes.int32)))
    val frameRdd = FrameRdd.toFrameRdd(schema, rdd)
    val frameReference = engine.frames.tryNewFrame(CreateEntityArgs(description = Some("created by PIC operation"))) { newPredictedFrame: FrameEntity =>
      newPredictedFrame.save(frameRdd)
    }
    val k = output.k
    val clusterSize = rdd.map(row => ("Cluster:" + row(1).toString, 1)).reduceByKey(_ + _).collect().toMap
    new PowerIterationClusteringReturn(frameReference, k, clusterSize)

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