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

//Implicits needed for JSON conversion

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Invocation, PluginDoc }
import org.apache.spark.frame.FrameRdd
import org.trustedanalytics.atk.engine.plugin.SparkCommandPlugin
import org.apache.spark.mllib.clustering.{ KMeansModel, KMeans }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

@PluginDoc(oneLine = "Creates KMeans Model from train frame.",
  extended = "Creating a KMeans Model using the observation columns.",
  returns = """dictionary
    A dictionary with trained KMeans model with the following keys\:
'cluster_size' : dictionary with 'Cluster:id' as the key and the corresponding cluster size is the value
'within_set_sum_of_squared_error' : The set of sum of squared error for the model.""")
class KMeansTrainPlugin extends SparkCommandPlugin[KMeansTrainArgs, KMeansTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:k_means/train"

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
  override def numberOfJobs(arguments: KMeansTrainArgs)(implicit invocation: Invocation) = 15
  /**
   * Run MLLib's LogisticRegressionWithSGD() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   * as well as a function that can be called to produce a SparkContext that
   * can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: KMeansTrainArgs)(implicit invocation: Invocation): KMeansTrainReturn = {
    val frame: SparkFrame = arguments.frame

    val kMeans = KMeansTrainPlugin.initializeKmeans(arguments)

    val trainFrameRdd = frame.rdd
    trainFrameRdd.cache()
    val vectorRDD = trainFrameRdd.toDenseVectorRDDWithWeights(arguments.observationColumns, arguments.columnScalings)
    val kmeansModel = kMeans.run(vectorRDD)
    val size = KMeansTrainPlugin.computeClusterSize(kmeansModel, trainFrameRdd, arguments.observationColumns, arguments.columnScalings)
    val withinSetSumOfSquaredError = kmeansModel.computeCost(vectorRDD)
    trainFrameRdd.unpersist()

    //Writing the kmeansModel as JSON
    val jsonModel = new KMeansData(kmeansModel, arguments.observationColumns, arguments.columnScalings)
    val model: Model = arguments.model
    model.data = jsonModel.toJson.asJsObject

    KMeansTrainReturn(size, withinSetSumOfSquaredError)
  }
}

/**
 * Constructs a KMeans instance with parameters passed or default parameters if not specified
 */
object KMeansTrainPlugin {
  def initializeKmeans(arguments: KMeansTrainArgs): KMeans = {
    val kmeans = new KMeans()

    kmeans.setK(arguments.k)
    kmeans.setMaxIterations(arguments.maxIterations)
    kmeans.setInitializationMode(arguments.initializationMode)
    kmeans.setEpsilon(arguments.epsilon)
  }

  def computeClusterSize(kmeansModel: KMeansModel, trainFrameRdd: FrameRdd, observationColumns: List[String], columnScalings: List[Double]): Map[String, Int] = {

    val predictRDD = trainFrameRdd.mapRows(row => {
      val array = row.valuesAsArray(observationColumns).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = columnScalings.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      val point = Vectors.dense(doubles)
      kmeansModel.predict(point)
    })
    predictRDD.map(row => ("Cluster:" + (row + 1).toString, 1)).reduceByKey(_ + _).collect().toMap
  }
}

