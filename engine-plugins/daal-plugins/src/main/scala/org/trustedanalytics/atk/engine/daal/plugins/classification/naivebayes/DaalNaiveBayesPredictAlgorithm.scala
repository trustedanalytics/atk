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
package org.trustedanalytics.atk.engine.daal.plugins.classification.naivebayes

import com.intel.daal.algorithms.ModelSerializer
import com.intel.daal.algorithms.classifier.prediction.{ ModelInputId, NumericTableInputId, PredictionResultId }
import com.intel.daal.algorithms.multinomial_naive_bayes.Model
import com.intel.daal.algorithms.multinomial_naive_bayes.prediction._
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.engine.daal.plugins.tables.{ DistributedNumericTable, IndexedNumericTable }

/**
 * Algorithm for computing predictions of naive bayes model
 *
 * @param modelData Trained naive bayes model
 * @param frameRdd Frame with test data
 * @param observationColumns List of column(s) storing the observations
 * @param predictColumn Name of column to store prediction
 */
case class DaalNaiveBayesPredictAlgorithm(modelData: DaalNaiveBayesModelData,
                                          frameRdd: FrameRdd,
                                          observationColumns: List[String],
                                          predictColumn: String) {
  /**
   * Predict naive bayes model
   *
   * @return Frame with predictions for naive bayes model
   */
  def predict(): FrameRdd = {

    val distributedTable = DistributedNumericTable.createTable(frameRdd, observationColumns)
    val predictRdd = distributedTable.rdd.flatMap(testData => {
      if (testData.isEmpty) {
        List.empty[sql.Row].iterator
      }
      else {
        val context = new DaalContext()
        val trainedModel = ModelSerializer.deserializeNaiveBayesModel(context, modelData.serializedModel.toArray)
        val predictions = predictTableResults(context, trainedModel, testData)
        val results = predictions.toRowIter(context)
        context.dispose()
        results
      }
    })

    val predictColumns = List(Column(predictColumn, DataTypes.float64))
    frameRdd.zipFrameRdd(new FrameRdd(FrameSchema(predictColumns), predictRdd))
  }

  /**
   * Predict results of naive bayes modelfor input table
   *
   * @param context DAAL context
   * @param trainedModel Trained linear model
   * @param testData Input table with test data
   *
   * @return Output table with predictions
   */
  private def predictTableResults(context: DaalContext,
                                  trainedModel: Model,
                                  testData: IndexedNumericTable): IndexedNumericTable = {
    val predictAlgorithm = new PredictionBatch(context, classOf[java.lang.Double],
      PredictionMethod.defaultDense, modelData.numClasses)
    val testTable = testData.getUnpackedTable(context)

    require(testTable.getNumberOfColumns > 0 && testTable.getNumberOfRows > 0)
    predictAlgorithm.input.set(NumericTableInputId.data, testTable)
    predictAlgorithm.input.set(ModelInputId.model, trainedModel)

    /* Compute and retrieve prediction results */
    val partialResult = predictAlgorithm.compute()

    val predictions = partialResult.get(PredictionResultId.prediction)
    new IndexedNumericTable(testData.index, predictions)
  }
}
