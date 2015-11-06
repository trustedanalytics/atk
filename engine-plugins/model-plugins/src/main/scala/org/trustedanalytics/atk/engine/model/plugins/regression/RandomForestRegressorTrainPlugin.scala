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

package org.trustedanalytics.atk.engine.model.plugins.regression

import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin._
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import MLLibJsonProtocol._

case class RandomForestRegressorTrainArgs(@ArgDoc("""Handle to the model to be used.""") model: ModelReference,
                                          @ArgDoc("""A frame to train the model on""") frame: FrameReference,
                                          @ArgDoc("""Column name containing the label for each observation""") labelColumn: String,
                                          @ArgDoc("""Column(s) containing the observations""") observationColumns: List[String],
                                          @ArgDoc("""Number of tress in the random forest. Default is 1.""") numTrees: Int = 1,
                                          @ArgDoc("""Criterion used for information gain calculation. Default supported value is "variance".""") impurity: String = "variance",
                                          @ArgDoc("""Maxium depth of the tree. Default is 4.""") maxDepth: Int = 4,
                                          @ArgDoc("""Maximum number of bins used for splitting features. Default is 100.""") maxBins: Int = 100,
                                          @ArgDoc("""Random seed for bootstrapping and choosing feature subsets. Default is a randomly chosen seed.""") seed: Int = scala.util.Random.nextInt(),
                                          @ArgDoc("""Arity of categorical features. Entry (n-> k) indicates that feature 'n' is categorical with 'k' categories indexed from 0:{0,1,...,k-1}""") categoricalFeaturesInfo: Option[Map[Int, Int]] = None,
                                          @ArgDoc("""Number of features to consider for splits at each node. Supported values "auto", "all", "sqrt","log2", "onethird".
If "auto" is set, this is based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1, set to "onethird".""") featureSubsetCategory: Option[String] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumn must not be null nor empty")
  require(labelColumn != null && !labelColumn.isEmpty, "labelColumn must not be null nor empty")
  require(numTrees > 0, "numTrees must be greater than 0")
  require(maxDepth >= 0, "maxDepth must be non negative")

  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    categoricalFeaturesInfo.getOrElse(Map[Int, Int]())
  }

  def getFeatureSubsetCategory: String = {
    var value = "all"
    value = featureSubsetCategory.getOrElse("all") match {
      case "auto" => {
        numTrees match {
          case 1 => "all"
          case _ => "onethird"
        }
      }
      case _ => featureSubsetCategory.getOrElse("all")
    }
    value
  }
}

@PluginDoc(oneLine = "Build Random Forests Regressor model.",
  extended = """Creating a Random Forests Regressor Model using the observation columns and target column.""",
  returns =
    """dictionary
      |A dictionary with trained Random Forest Regressor model with the following keys\:
      |'observation_columns': the list of observation columns on which the model was trained
      |'label_columns': the column name containing the labels of the observations
      |'num_trees': the number of decision trees in the random forest
      |'num_nodes': the number of nodes in the random forest
      |'categorical_features_info': the map storing arity of categorical features
      |'impurity': the criterion used for information gain calculation
      |'max_depth': the maximum depth of the tree
      |'max_bins': the maximum number of bins used for splitting features
      |'seed': the random seed used for bootstrapping and choosing featur subset
    """)
class RandomForestRegressorTrainPlugin extends SparkCommandPlugin[RandomForestRegressorTrainArgs, RandomForestRegressorTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:random_forest_regressor/train"

  override def apiMaturityTag = Some(ApiMaturityTag.Alpha)

  /**
   * Number of Spark jobs that get created by running this command
   * (this configuration is used to prevent multiple progress bars in Python client)
   */
  override def numberOfJobs(arguments: RandomForestRegressorTrainArgs)(implicit invocation: Invocation) = 109

  /**
   * Run MLLib's RandomForest trainRegressor on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: RandomForestRegressorTrainArgs)(implicit invocation: Invocation): RandomForestRegressorTrainReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    //create RDD from the frame
    val labeledTrainRdd: RDD[LabeledPoint] = frame.rdd.toLabeledPointRDD(arguments.labelColumn, arguments.observationColumns)
    val randomForestModel = RandomForest.trainRegressor(labeledTrainRdd, arguments.getCategoricalFeaturesInfo, arguments.numTrees,
      arguments.getFeatureSubsetCategory, arguments.impurity, arguments.maxDepth, arguments.maxBins, arguments.seed)
    val jsonModel = new RandomForestRegressorData(randomForestModel, arguments.observationColumns)

    model.writeToStorage(jsonModel.toJson.asJsObject)
    new RandomForestRegressorTrainReturn(arguments.observationColumns, arguments.labelColumn, randomForestModel.numTrees, randomForestModel.totalNumNodes,
      arguments.getFeatureSubsetCategory, arguments.impurity, arguments.maxDepth, arguments.maxBins, arguments.seed)
  }
}
