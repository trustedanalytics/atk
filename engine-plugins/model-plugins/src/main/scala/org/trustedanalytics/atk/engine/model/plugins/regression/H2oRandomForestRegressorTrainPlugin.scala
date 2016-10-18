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

import java.io.File
import java.nio.file.{ Paths, Files }
import java.util.Date

import hex.tree.TreeStats
import hex.VarImp
import hex.tree.drf.DRFModel.DRFParameters
import org.apache.commons.io.FileUtils
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.h2o.H2OFrame
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.frame.Frame
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
                                             @ArgDoc("""Minimum number of rows to assign to terminal nodes.""") minRows: Int = 2,
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
  require(maxDepth > 0 && maxDepth <= 200, "maxDepth must be greater than 0 and less than 200")

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
  def getMtries: Int = {
    val ncols = observationColumns.length
    featureSubsetCategory match {
      case "auto" => -1
      case "sqrt" => Math.max(Math.sqrt(ncols).ceil.toInt, 1)
      case "onethird" => Math.max((ncols / 3.0).ceil.toInt, 1)
      case "log2" => Math.max((math.log(ncols) / math.log(2)).ceil.toInt, 1)
      case "all" => ncols
      case _ => throw new IllegalArgumentException("""Feature subset category must be "auto", "all", "sqrt", "log2", "onethird"""")
    }
  }

  /**
   * Get columns to ignore during training
   * @param frameColumns Columns in training frame
   * @return Columns to ignore
   */
  def getIgnoredColumns(frameColumns: Array[String]): Array[String] = {
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

@PluginDoc(oneLine = "Build H2O Random Forests Regressor.",
  extended =
    """Creating a Random Forests Regressor Model using the observation columns and target column.
      H2O's implementation of distributed random forest is slow for large trees due to the
      overhead of shipping histograms across the network. This plugin runs H2O random forest
      on a multiple nodes for larger datasets.
      https://groups.google.com/forum/#!searchin/h2ostream/histogram%7Csort:relevance/h2ostream/bnyhPyxftX8/0d1ItQiyH98J
    """,
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
class H2oRandomForestRegressorDistributedTrainPlugin extends SparkCommandPlugin[H2oRandomForestRegressorTrainArgs, H2oRandomForestRegressorTrainReturn] {
  //The Python API for this plugin is made available through the H2oRandomForestRegressor Python wrapper class
  //The H2oRandomForestRegressor wrapper has a train() method that calls a local or distributed train
  //depending on the size of the data since H2O's distributed random forest is slow for large trees
  //https://groups.google.com/forum/#!searchin/h2ostream/histogram%7Csort:relevance/h2ostream/bnyhPyxftX8/0d1ItQiyH98J
  override def name: String = "model:h2o_random_forest_regressor_private/_distributed_train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

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

    H2oRandomForestRegressorFunctions.train(frame.rdd, arguments, model)
  }
}

@PluginDoc(oneLine = "Build H2O Random Forests Regressor model using local Spark context.",
  extended =
    """Creating a Random Forests Regressor Model using the observation columns and target column.
       H2O's implementation of distributed random forest is slow for large trees due to the
       overhead of shipping histograms across the network. This plugin runs H2O random forest
       in a single node for small datasets.
      https://groups.google.com/forum/#!searchin/h2ostream/histogram%7Csort:relevance/h2ostream/bnyhPyxftX8/0d1ItQiyH98J
    """,
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
class H2oRandomForestRegressorLocalTrainPlugin extends CommandPlugin[H2oRandomForestRegressorTrainArgs, H2oRandomForestRegressorTrainReturn] {
  //The Python API for this plugin is made available through the H2oRandomForestRegressor Python wrapper class
  //The H2oRandomForestRegressor wrapper has a train() method that calls a local or distributed train
  //depending on the size of the data since H2O's distributed random forest is slow for large trees
  //https://groups.google.com/forum/#!searchin/h2ostream/histogram%7Csort:relevance/h2ostream/bnyhPyxftX8/0d1ItQiyH98J
  override def name: String = "model:h2o_random_forest_regressor_private/_local_train"

  override def apiMaturityTag = Some(ApiMaturityTag.Beta)

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
    val frame: Frame = arguments.frame
    val model: Model = arguments.model
    var sc: SparkContext = null
    try {
      sc = createLocalSparkContext()
      val sqlContext = new SQLContext(sc)
      val dataframe = sqlContext.read.load(frame.entity.getStorageLocation)
      H2oRandomForestRegressorFunctions.train(FrameRdd.toFrameRdd(dataframe), arguments, model)
    }
    finally {
      cleanupSpark(sc)
    }
  }

  private def createLocalSparkContext(): SparkContext = {
    // LogUtils.silenceSpark()
    val appName = this.getClass.getSimpleName + "_" + new Date()
    val tmpDir = System.getProperty("java.io.tmpdir")
    val sparkLocalDir = tmpDir + "/" + appName + "/spark-local"
    val h2oLogDir = tmpDir + "/" + appName + "/h2o-logs"
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName + " " + new Date())
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.ext.h2o.repl.enabled", "false")
    conf.set("spark.local.dir", sparkLocalDir)
    conf.set("spark.ext.h2o.client.log.dir", h2oLogDir)
    conf.set("spark.driver.memory", EngineConfig.sparkConfProperties.getOrElse("spark.driver.memory", "1g"))

    new SparkContext(conf)
  }

  /**
   * Shutdown spark and release the lock
   */
  private def cleanupSpark(sc: SparkContext): Unit = {
    try {
      if (sc != null) {
        val conf = sc.getConf
        sc.stop()
        if (Files.isDirectory(Paths.get(conf.get("spark.local.dir")))) {
          FileUtils.deleteDirectory(new File(conf.get("spark.local.dir")))
        }
        if (Files.isDirectory(Paths.get(conf.get("spark.ext.h2o.client.log.dir")))) {
          FileUtils.deleteDirectory(new File(conf.get("spark.ext.h2o.client.log.dir")))
        }
      }
    }
    finally {
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
  }

}