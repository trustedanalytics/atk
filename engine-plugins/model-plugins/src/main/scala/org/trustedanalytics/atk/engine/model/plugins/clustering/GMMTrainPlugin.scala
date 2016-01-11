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

//Implicits needed for JSON conversion

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.{ GaussianMixtureModel, GaussianMixture}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import scala.Long
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Creates GMM Model from train frame.",
  extended = "Upon training the 'k' cluster centers are computed.",
  returns = """dict
    Returns a dictionary storing the number of elements belonging to each Gaussian mixture
cluster_size : dict
    Cluster size
ClusterId : int
    Number of elements in the cluster 'ClusterId'.
""")
class GMMTrainPlugin extends SparkCommandPlugin[GMMTrainArgs, scala.collection.Map[Int, scala.Long]] {
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
  override def numberOfJobs(arguments: GMMTrainArgs)(implicit invocation: Invocation) = 15
  /**
   * Run MLLib's GaussianMixtureModel() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: GMMTrainArgs)(implicit invocation: Invocation): scala.collection.Map[Int, scala.Long] = {
    val frame: SparkFrame = arguments.frame

    val gmm = initializeGMM(arguments)

    val trainFrameRdd = frame.rdd
    trainFrameRdd.cache()
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(arguments.observationColumns, arguments.columnScalings)
    val gmmModel = gmm.run(vectorRDD)
    //val size = computeClusterSize(gmmModel, vectorRDD, arguments.observationColumns, arguments.columnScalings)
    trainFrameRdd.unpersist()

    //Writing the gmmModel as JSON
    val jsonModel = new GMMData(gmmModel, arguments.observationColumns, arguments.columnScalings)
    val model: Model = arguments.model
    model.data = jsonModel.toJson.asJsObject

    computeClusterSize(gmmModel, vectorRDD, arguments.observationColumns, arguments.columnScalings)
  }

  /**
   * Constructs a GaussianMixture instance with parameters passed or default parameters if not specified
   */
  private def initializeGMM(arguments: GMMTrainArgs): GaussianMixture = {
    val gmm = new GaussianMixture()

    gmm.setK(arguments.k)
    gmm.setMaxIterations(arguments.maxIterations)
    gmm.setConvergenceTol(arguments.convergenceTol)
    gmm.setSeed(arguments.seed)
  }

  private def computeClusterSize(gmmModel: GaussianMixtureModel, vectorRDD: RDD[org.apache.spark.mllib.linalg.Vector], observationColumns: List[String], columnScalings: List[Double]): scala.collection.Map[Int, scala.Long] = {

    val clusterAssignment = gmmModel.predict(vectorRDD)
    clusterAssignment.countByValue()
  }
}
