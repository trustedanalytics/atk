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

import hex.tree.TreeStats
import hex.VarImp
import hex.tree.drf.{ DRFModel, DRF }
import hex.tree.drf.DRFModel.DRFParameters
import org.apache.spark.h2o._
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.plugin._
import water.H2O

//Implicits for JSON conversion
import org.apache.spark.h2o.H2oJsonProtocol._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

/**
 * Arguments to H2O Random Forest Regression train plugin
 */
case class H2oRandomForestRegressorTrainArgs(@ArgDoc("""Handle to the model to be used.""") model: ModelReference,
                                             @ArgDoc("""A frame to train the model on""") frame: FrameReference,
                                             @ArgDoc("""Column name containing the value for each observation""") valueColumn: String,
                                             @ArgDoc("""Column(s) containing the observations""") observationColumns: List[String],
                                             @ArgDoc("""Number of trees in the random forest.""") numTrees: Int = 50,
                                             @ArgDoc("""Maximum depth of the tree.""") maxDepth: Int = 20,
                                             @ArgDoc("""For numerical columns (real/int), build a histogram of (at least) this many bins.""") numBins: Int = 20,
                                             @ArgDoc("""Minimum number of rows to assign to terminal nodes.""") minRows: Int = 10,
                                             @ArgDoc(
                                               """Number of features to consider for splits at each node. Supported values "auto", "all", "sqrt", "onethird".
If "auto" is set, this is based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1, set to "onethird".""") featureSubsetCategory: String = "auto",
                                             @ArgDoc("""Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded.""") seed: Option[Int] = None,
                                             @ArgDoc("""Row sample rate per tree (from 0.0 to 1.0).""") sampleRate: Option[Double] = None) {
  require(model != null, "model is required")
  require(frame != null, "frame is required")
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumn must not be null nor empty")
  require(valueColumn != null && valueColumn.trim != "", "valueColumn must not be null nor empty")
  require(numTrees > 0, "numTrees must be greater than 0")
  //Max-depth is based on the private variable for hex.tree.TreeJCodeGen.MAX_DEPTH for generating POJOs
  require(maxDepth > 0 && maxDepth <= 70, "maxDepth must be greater than 0 and less than 70")

  /**
   * Get H2O random forest model parameters
   *
   * @param h2oFrame Training frame
   * @return Model parameters
   */
  def getDrfParameters(h2oFrame: H2OFrame): DRFParameters = {
    val forestParams = new DRFParameters()
    val ignoredCols = getIgnoredColumns(h2oFrame.names())
    forestParams._train = h2oFrame._key
    forestParams._response_column = valueColumn
    if (ignoredCols.size > 0) {
      forestParams._ignored_columns = ignoredCols
    }
    forestParams._ntrees = numTrees
    forestParams._max_depth = maxDepth
    forestParams._nbins = numBins
    forestParams._min_rows = minRows
    forestParams._mtries = getMtries
    if (seed.isDefined) {
      forestParams._seed = seed.get
    }
    if (sampleRate.isDefined) {
      forestParams._sample_rate = sampleRate.get
    }
    forestParams
  }

  /**
   * Get the number of variables randomly sampled as candidates at each split
   */
  private def getMtries: Int = {
    val ncols = observationColumns.length
    featureSubsetCategory match {
      case "auto" => -1
      case "sqrt" => Math.max(Math.sqrt(ncols).toInt, 1)
      case "onethird" => Math.max(ncols / 3, 1)
      case "all" => ncols
      case _ => throw new IllegalArgumentException("""Feature subset category must be "auto", "all", "sqrt", "onethird"""")
    }
  }

  /**
   * Get columns to ignore during training
   * @param frameColumns Columns in training frame
   * @return Columns to ignore
   */
  private def getIgnoredColumns(frameColumns: Array[String]): Array[String] = {
    frameColumns.diff(observationColumns :+ valueColumn)
  }
}

/**
 * Results for H2O Random Forest Regression Train plugin
 */
case class H2oRandomForestRegressorTrainReturn(@ArgDoc("""Column name containing the value for each observation""") valueColumn: String,
                                               @ArgDoc("""Column(s) containing the observations""") observationColumns: List[String],
                                               @ArgDoc("""Number of trees in the random forest.""") numTrees: Int,
                                               @ArgDoc("""Maximum depth of the tree.""") maxDepth: Int,
                                               @ArgDoc("""For numerical columns, build a histogram of at least this many bins.""") numBins: Int,
                                               @ArgDoc("""Minimum number of rows to assign to terminal nodes.""") minRows: Int,
                                               @ArgDoc(
                                                 """Number of features to consider for splits at each node. Supported values "auto", "all", "sqrt", "onethird".
If "auto" is set, this is based on numTrees: if numTrees == 1, set to "all"; if numTrees > 1, set to "onethird".""") featureSubsetCategory: String = "auto",
                                               @ArgDoc("""Tree statistics""") treeStats: Map[String, Float],
                                               @ArgDoc("""Variable importances""") varimp: Map[String, Float]) {
}

object H2oRandomForestRegressorTrainReturn {

  /**
   * Create results for H2O random forest regression training plugin
   * @param args Input arguments
   * @param varimp Variable importances
   * @param treeStats Tree statistics
   * @return Train results
   */
  def apply(args: H2oRandomForestRegressorTrainArgs, varimp: VarImp, treeStats: TreeStats): H2oRandomForestRegressorTrainReturn = {
    val varImpMap = varimp._names.zip(varimp._varimp).map { case (name, imp) => (name, imp) }.toMap

    val treeStatsMap: Map[String, Float] = Map(
      "min_depth" -> treeStats._min_depth,
      "mean_depth" -> treeStats._mean_depth,
      "max_depth" -> treeStats._max_depth,
      "min_leaves" -> treeStats._min_leaves,
      "mean_leaves" -> treeStats._mean_leaves,
      "max_leaves" -> treeStats._max_leaves)

    H2oRandomForestRegressorTrainReturn(args.valueColumn, args.observationColumns, args.numTrees, args.maxDepth,
      args.numBins, args.minRows, args.featureSubsetCategory, treeStatsMap, varImpMap)
  }
}

/** Json conversion for arguments and return value case classes */
object H2oRandomForestRegressorTrainJsonFormat {
  implicit val drfFormat = jsonFormat11(H2oRandomForestRegressorTrainArgs)
  implicit val drfResultFormat = jsonFormat9(H2oRandomForestRegressorTrainReturn.apply)
}
import H2oRandomForestRegressorTrainJsonFormat._

@PluginDoc(oneLine = "Build Random Forests Regressor model.",
  extended = """Creating a Random Forests Regressor Model using the observation columns and target column.""",
  returns =
    """object
      An object with the results of the trained Random Forest Regressor:
      |'value_column': the column name containing the value of each observation,
      |'observation_columns': the list of observation columns on which the model was trained,
      |'num_trees': the number of decision trees in the random forest,
      |'max_depth': the maximum depth of the tree,
      |'num_bins': for numerical columns, build a histogram of at least this many bins
      |'min_rows': number of features to consider for splits at each node
      |'feature_subset_category': number of features to consider for splits at each node,
      |'tree_stats': dictionary with tree statistics for trained model,
      |'varimp': variable importances
    """)
class H2oRandomForestRegressorTrainPlugin extends SparkCommandPlugin[H2oRandomForestRegressorTrainArgs, H2oRandomForestRegressorTrainReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:h2o_random_forest_regressor/train"

  /**
   * Run H2O's RandomForest trainRegressor on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: H2oRandomForestRegressorTrainArgs)(implicit invocation: Invocation): H2oRandomForestRegressorTrainReturn = {
    val frame: SparkFrame = arguments.frame
    val model: Model = arguments.model

    // Run H2O cluster inside Spark cluster
    val h2oContext = AtkH2OContext.init(sc)
    var h2oFrame: H2OFrame = null
    var drfModel: DRFModel = null

    import h2oContext.implicits._

    // Train model
    try {
      h2oFrame = frame.rdd.toDataFrame
      val drfParams = arguments.getDrfParameters(h2oFrame)
      val drfJob = new DRF(drfParams)
      drfModel = drfJob.trainModel().get()
      val drfModelOutput = drfModel._output
      val modelData = new H2oModelData(drfModel, arguments.valueColumn, arguments.observationColumns)
      model.writeToStorage(modelData.toJson.asJsObject)
      H2oRandomForestRegressorTrainReturn(arguments, drfModelOutput._varimp, drfModelOutput._treeStats)
    }
    catch {
      case e: Exception => throw new RuntimeException("Error training random forest model: " + e.getMessage(), e)
    }
    finally {
      if (h2oFrame != null) h2oFrame.remove()
      if (drfModel != null) drfModel.delete()
      H2O.orderlyShutdown(1000)
    }
  }
}
