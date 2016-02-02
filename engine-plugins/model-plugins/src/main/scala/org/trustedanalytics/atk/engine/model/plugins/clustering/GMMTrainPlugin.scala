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

package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.model.plugins.MatrixImplicits
import MatrixImplicits._
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.{ GaussianMixtureModel, GaussianMixture }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import scala.Long
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Creates a GMM Model from the train frame.",
  extended = "At training the 'k' cluster centers are computed.",
  returns = """dict
    Returns a dictionary the following fields
cluster_size : dict
    with the key being a string of the form 'Cluster:Id' storing the number of elements in cluster number 'Id'
gaussians : dict
    Stores the 'mu' and 'sigma' corresponding to the Multivariate Gaussian (Normal) Distribution for each Gaussian
""")
class GMMTrainPlugin extends SparkCommandPlugin[GMMTrainArgs, GMMTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:gmm/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

  /**
   * User documentation exposed in Python.
   *
   * [[http://docutils.sourceforge.net/rst.html ReStructuredText]]
   */

  /**
   * Number of Spark jobs that get created by running this command
   *
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: GMMTrainArgs)(implicit invocation: Invocation) = 60

  /**
   * Run MLLib's GaussianMixtureModel() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GMMTrainArgs)(implicit invocation: Invocation): GMMTrainReturn = {
    val frame: SparkFrame = arguments.frame

    val gmm = GMMTrainPlugin.initializeGMM(arguments)

    val trainFrameRdd = frame.rdd
    trainFrameRdd.cache()
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(arguments.observationColumns, arguments.columnScalings)
    val gmmModel = gmm.run(vectorRDD)
    trainFrameRdd.unpersist()

    //Writing the gmmModel as JSON
    val jsonModel = new GMMData(gmmModel, arguments.observationColumns, arguments.columnScalings)
    val model: Model = arguments.model
    model.data = jsonModel.toJson.asJsObject

    val gaussians = gmmModel.gaussians.map(i => ("mu:" + i.mu.toString, "sigma:" + i.sigma.toListOfList))
    new GMMTrainReturn(GMMTrainPlugin.computeGmmClusterSize(gmmModel, vectorRDD), gmmModel.weights.toList, gaussians)
  }
}

object GMMTrainPlugin {
  /**
   * Constructs a GaussianMixture instance with parameters passed or default parameters if not specified
   * @param arguments Arguments passed to GMM train plugin
   * @return An initialized Gaussian Mixture Model
   */
  def initializeGMM(arguments: GMMTrainArgs): GaussianMixture = {
    val gmm = new GaussianMixture()

    gmm.setK(arguments.k)
    gmm.setMaxIterations(arguments.maxIterations)
    gmm.setConvergenceTol(arguments.convergenceTol)
    gmm.setSeed(arguments.seed)
  }

  /**
   * Computes the number of elements belonging to each GMM cluster
   * @param gmmModel The trained GMM Model
   * @param vectorRdd RDD storing the observations as a Vector
   * @return A map storing the cluster number and number of elements it contains
   */
  def computeGmmClusterSize(gmmModel: GaussianMixtureModel, vectorRdd: RDD[org.apache.spark.mllib.linalg.Vector]): Map[String, Int] = {
    val predictRDD = gmmModel.predict(vectorRdd)
    predictRDD.map(row => ("Cluster:" + row.toString, 1)).reduceByKey(_ + _).collect().toMap
  }
}
