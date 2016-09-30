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

import hex.tree.drf.{ DRF, DRFModel }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.h2o._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.domain.schema.{ FrameSchema, DataTypes, Column }
import org.trustedanalytics.atk.engine.model.Model
import water.H2O
import scala.collection.JavaConverters._
import org.trustedanalytics.atk.engine.model.plugins.ModelPluginImplicits._
import scala.collection.mutable.ArrayBuffer

//Implicits for JSON conversion
import org.apache.spark.h2o.H2oJsonProtocol._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import spray.json._

object H2oRandomForestRegressorFunctions extends Serializable {

  def train(inputFrame: FrameRdd, args: H2oRandomForestRegressorTrainArgs, model: Model): H2oRandomForestRegressorTrainReturn = {
    val h2oContext = AtkH2oContext.init(inputFrame.sparkContext)
    var h2oFrame: H2OFrame = null
    var drfModel: DRFModel = null

    import h2oContext.implicits._

    try {
      //Convert to H2O Frame
      val columns = args.valueColumn +: args.observationColumns
      val rowRdd = inputFrame.mapRows(row => {
        Row.fromSeq(row.valuesAsDoubleArray(columns).toSeq)
      })
      val schema = FrameSchema(columns.map(col => Column(col, DataTypes.float64)))
      val trainFrame = new FrameRdd(schema, rowRdd)
      h2oFrame = trainFrame.toDataFrame

      //Train model
      val drfParams = args.getDrfParameters(h2oFrame)
      val drfJob = new DRF(drfParams)
      drfModel = drfJob.trainModel().get()
      val drfModelOutput = drfModel._output
      val modelData = new H2oModelData(drfModel, args.valueColumn, args.observationColumns)
      model.writeToStorage(modelData.toJson.asJsObject)
      H2oRandomForestRegressorTrainReturn(args, drfModelOutput._varimp, drfModelOutput._treeStats)
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

  /**
   * Predict value for the observation columns using trained random forest model
   *
   * @param inputFrame Training frame
   * @param h2oModelData Trained model
   * @param obsColumns Observation columns
   * @return Frame with predicted value
   */
  def predict(inputFrame: FrameRdd, h2oModelData: H2oModelData, obsColumns: List[String]): FrameRdd = {
    val predictColumn = Column("predicted_value", DataTypes.float64)
    val predictRows = inputFrame.mapPartitionRows(rows => {
      val scores = new ArrayBuffer[Row]()
      val preds: Array[Double] = new Array[Double](1)
      val genModel = h2oModelData.toGenModel

      while (rows.hasNext) {
        val row = rows.next()
        val point = row.valuesAsDenseVector(obsColumns).toArray
        val fields = obsColumns.zip(point).map { case (name, value) => (name, double2Double(value)) }.toMap
        val score = genModel.score0(fields.asJava, preds)
        scores += row.addValue(score(0))
      }
      scores.toIterator
    })

    new FrameRdd(inputFrame.frameSchema.addColumn(predictColumn), predictRows)
  }

  /**
   * Get regression metrics using trained model
   * @param inputFrame Input frame with predicted and labeled data
   * @param h2oModelData Trained random forest regression model
   * @param obsColumns List of observation columns
   * @param valueColumn Name of value column
   * @return Regression metrics
   */
  def getRegressionMetrics(inputFrame: FrameRdd,
                           h2oModelData: H2oModelData,
                           obsColumns: List[String],
                           valueColumn: String): H2oRandomForestRegressorTestReturn = {
    val predictFrame = H2oRandomForestRegressorFunctions.predict(inputFrame, h2oModelData, obsColumns)
    val predictionLabelRdd = predictFrame.mapRows(row => {
      (row.doubleValue("predicted_value"), row.doubleValue(valueColumn))
    })

    val metrics = new RegressionMetrics(predictionLabelRdd)
    val explainedVarianceScore = getExplainedVarianceScore(predictionLabelRdd)
    H2oRandomForestRegressorTestReturn(metrics.meanAbsoluteError, metrics.meanSquaredError,
      metrics.rootMeanSquaredError, metrics.r2, explainedVarianceScore)
  }

  /**
   * Get explained variance score
   *
   * Explained variance score = 1 - (variance(label-prediction)/variance(label))
   * @param predictionLabelRdd RDD of predicted and label values
   * @return Explained variance score
   */
  def getExplainedVarianceScore(predictionLabelRdd: RDD[(Double, Double)]): Double = {
    val summary = predictionLabelRdd.aggregate(new MultivariateOnlineSummarizer())(
      (summary, predLabel) => {
        val (prediction, label) = predLabel
        summary.add(Vectors.dense(label, label - prediction))
      },
      (sum1, sum2) => sum1.merge(sum2))
    val variance = summary.variance
    val score = 1d - (variance(1) / variance(0))
    score
  }
}
