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

import org.apache.spark.frame.FrameRdd
import org.apache.spark.h2o.H2oModelData
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Column }
import org.trustedanalytics.atk.engine.ArgDocAnnotation
import org.trustedanalytics.atk.engine.frame.SparkFrame
import org.trustedanalytics.atk.engine.model.Model
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation, SparkCommandPlugin }
import org.trustedanalytics.atk.scoring.models.LinearRegressionData
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

//Implicits for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.apache.spark.h2o.H2oJsonProtocol._

/**
 * Arguments to H2O Random Forest Regression test plugin
 * @param model The trained random forest regression model to run test on
 * @param frame The frame to test the random forest regression model on
 * @param valueColumn Frame's column containing the value for the observation
 * @param observationColumns Frame's column(s) containing the observations
 */
case class H2oRandomForestRegressorTestArgs(model: ModelReference,
                                            @ArgDoc("""The frame to test the random forest regression model on""") frame: FrameReference,
                                            @ArgDoc("""Column name containing the value of each observation""") valueColumn: String,
                                            @ArgDoc("""List of column(s) containing the observations""") observationColumns: Option[List[String]])

/**
 * Return of random forest Regression test plugin
 * @param explainedVariance The explained variance regression score
 * @param meanAbsoluteError The risk function corresponding to the expected value of the absolute error loss or l1-norm loss
 * @param meanSquaredError The risk function corresponding to the expected value of the squared error loss or quadratic loss
 * @param r2 The coefficient of determination
 * @param rootMeanSquaredError The square root of the mean squared error
 */
case class H2oRandomForestRegressorTestReturn(@ArgDoc("""The explained variance regression score""") explainedVariance: Double,
                                              @ArgDoc("""The risk function corresponding to the expected value of the absolute error loss or l1-norm loss""") meanAbsoluteError: Double,
                                              @ArgDoc("""The risk function corresponding to the expected value of the squared error loss or quadratic loss""") meanSquaredError: Double,
                                              @ArgDoc("""The unadjusted coefficient of determination""") r2: Double,
                                              @ArgDoc("""The square root of the mean squared error""") rootMeanSquaredError: Double)

/** Json conversion for arguments and return value case classes */
object H2oRandomForestRegressorTestJsonFormat {
  implicit val drfTestArgsFormat = jsonFormat4(H2oRandomForestRegressorTestArgs)
  implicit val drfTestReturnFormat = jsonFormat5(H2oRandomForestRegressorTestReturn)
}
import H2oRandomForestRegressorTestJsonFormat._

class H2oRandomForestRegressorTestPlugin extends SparkCommandPlugin[H2oRandomForestRegressorTestArgs, H2oRandomForestRegressorTestReturn] {
  /**
   * The name of the command.
   *
   * The format of the name determines how the plugin gets "installed" in the client layer
   * e.g Python client via code generation.
   */
  override def name: String = "model:h2o_random_forest_regressor/test"

  /**
   * Run Spark ML's H2oRandomForestRegressor() on the training frame and create a Model for it.
   *
   * @param invocation information about the user and the circumstances at the time of the call,
   *                   as well as a function that can be called to produce a SparkContext that
   *                   can be used during this invocation.
   * @param arguments user supplied arguments to running this plugin
   * @return a value of type declared as the Return type.
   */
  override def execute(arguments: H2oRandomForestRegressorTestArgs)(implicit invocation: Invocation): H2oRandomForestRegressorTestReturn = {

    val model: Model = arguments.model
    val frame: SparkFrame = arguments.frame

    val h2oModelData = model.readFromStorage().convertTo[H2oModelData]
    if (arguments.observationColumns.isDefined) {
      require(h2oModelData.observationColumns.length == arguments.observationColumns.get.length, "Number of columns for train and predict should be same")
    }
    val obsColumns = arguments.observationColumns.getOrElse(h2oModelData.observationColumns)

    //predicting a label for the observation columns
    val predictColumn = Column("predicted_value", DataTypes.float64)
    val predictRows = frame.rdd.mapPartitionRows(rows => {
      val genModel = h2oModelData.toGenModel
      val scores = new ArrayBuffer[Row]()
      val preds: Array[Double] = new Array[Double](1)

      while (rows.hasNext) {
        val row = rows.next()
        val point = row.valuesAsDenseVector(obsColumns).toArray
        val fields = obsColumns.zip(point).map { case (name, value) => (name, double2Double(value)) }.toMap
        val score = genModel.score0(fields.asJava, preds)
        scores += row.addValue(score(0))
      }
      scores.toIterator
    })

    val predictFrame = new FrameRdd(frame.schema.addColumn(predictColumn), predictRows)

    val predictionLabelRdd = predictFrame.mapRows(row => (row.doubleValue("predicted_value"), row.doubleValue(arguments.valueColumn)))
    val metrics = new RegressionMetrics(predictionLabelRdd)

    new H2oRandomForestRegressorTestReturn(metrics.explainedVariance, metrics.meanAbsoluteError, metrics.meanSquaredError, metrics.r2, metrics.rootMeanSquaredError)
  }
}