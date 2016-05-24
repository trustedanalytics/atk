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
package org.trustedanalytics.atk.scoring.models

import java.lang

import com.intel.daal.algorithms.ModelSerializer
import com.intel.daal.algorithms.multinomial_naive_bayes.prediction.{ PredictionMethod, PredictionBatch }
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import com.intel.daal.algorithms.classifier.prediction.{ ModelInputId, NumericTableInputId, PredictionResultId }
import com.intel.daal.algorithms.multinomial_naive_bayes.{ Model => DaalNaiveBayesModel }

import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }

/**
 * Scoring model for Intel DAAL Naive Bayes
 *
 * @param modelData trained model
 */
class DaalNaiveBayesScoreModel(modelData: DaalNaiveBayesModelData) extends Model {

  override def score(data: Array[Any]): Array[Any] = {
    val features: Array[Double] = data.map(y => ScoringModelUtils.asDouble(y))
    val prediction = predict(features)

    data :+ (prediction)
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = modelData.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  /**
   * @return Metadata for Intel DAAL naive bayes scoring model
   */
  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs(
      "Intel DAAL Naive Bayes Model",
      classOf[DaalNaiveBayesScoreModel].getName,
      classOf[DaalNaiveBayesReaderPlugin].getName,
      Map())
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Double")
  }

  /**
   * Predict label using Intel DAAL naive bayes model
   * @param features Array with input features
   * @return score
   */
  private def predict(features: Array[Double]): Double = {
    val context = new DaalContext()
    var prediction: Double = Double.NaN

    try {
      val predictAlgorithm = new PredictionBatch(context, classOf[lang.Double],
        PredictionMethod.defaultDense, modelData.numClasses)
      val testTable = new HomogenNumericTable(context, features, features.length, 1L)
      val trainedModel = ModelSerializer.deserializeNaiveBayesModel(context, modelData.serializedModel.toArray)
      predictAlgorithm.input.set(NumericTableInputId.data, testTable)
      predictAlgorithm.input.set(ModelInputId.model, trainedModel)

      val alphaParameter = DaalNaiveBayesParameters.getAlphaParameter(context,
        modelData.lambdaParameter, modelData.observationColumns.length)
      predictAlgorithm.parameter.setAlpha(alphaParameter)
      if (modelData.classPrior.isDefined) {
        val priorParameter = DaalNaiveBayesParameters.getClassPriorParameter(context, modelData.classPrior.get)
        predictAlgorithm.parameter.setPriorClassEstimates(priorParameter)
      }

      /* Compute and retrieve prediction results */
      val partialResult = predictAlgorithm.compute()

      val predictions = partialResult.get(PredictionResultId.prediction).asInstanceOf[HomogenNumericTable]
      prediction = predictions.getDoubleArray.head
    }
    catch {
      case ex: Exception => throw new RuntimeException("Could not score model:", ex)
    }
    finally {
      context.dispose()
    }
    prediction
  }
}
