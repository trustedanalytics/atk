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
import com.intel.daal.algorithms.classifier.training.{ InputId, TrainingDistributedInputId, TrainingResultId }
import com.intel.daal.algorithms.multinomial_naive_bayes.Model
import com.intel.daal.algorithms.multinomial_naive_bayes.training._
import com.intel.daal.services.DaalContext
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.trustedanalytics.atk.engine.daal.plugins.DistributedAlgorithm
import org.trustedanalytics.atk.engine.daal.plugins.tables.DistributedLabeledTable

import scala.util.{ Failure, Success, Try }

/**
 * Train Intel DAAL naive bayes model using QR decomposition
 *
 * @param frameRdd Input frame
 * @param observationColumns Feature columns
 * @param labelColumn Label column
 * @param numClasses Number of classes
 * @param alpha Imagined occurrences of features
 */
case class DaalNaiveBayesTrainAlgorithm(frameRdd: FrameRdd,
                                        observationColumns: List[String],
                                        labelColumn: String,
                                        numClasses: Int,
                                        alpha: Double = 1d) extends DistributedAlgorithm[TrainingPartialResult, TrainingResult] {
  private val trainTables = DistributedLabeledTable.createTable(frameRdd, observationColumns, List(labelColumn))

  /**
   * Train Intel DAAL naive bayes model using QR decomposition
   * @return Trained naive bayes model
   */
  def train(): DaalNaiveBayesModelData = {
    val context = new DaalContext

    //train model
    val partialResultsRdd = computePartialResults()
    val trainingResult = mergePartialResults(context, partialResultsRdd)
    val trainedModel: Model = trainingResult.get(TrainingResultId.model)

    // serialize model and return results
    val serializedModel = serializeTrainedModel(trainedModel)
    context.dispose()
    DaalNaiveBayesModelData(serializedModel, observationColumns, labelColumn, numClasses, alpha)
  }

  /**
   * Compute partial results for multinomial Naive Bayes model
   *
   * @return Partial result of training
   */
  override def computePartialResults(): RDD[TrainingPartialResult] = {

    val oartialResultRdd = trainTables.rdd.map(table => {
      val context = new DaalContext()
      val featureTable = table.features
      val labelTable = table.labels
      val naiveBayesTraining = new TrainingDistributedStep1Local(context, classOf[java.lang.Double],
        TrainingMethod.defaultDense, numClasses)
      naiveBayesTraining.input.set(InputId.data, featureTable.getUnpackedTable(context))
      naiveBayesTraining.input.set(InputId.labels, labelTable.getUnpackedTable(context))

      val partialResult = naiveBayesTraining.compute()
      partialResult.pack()
      context.dispose()
      partialResult
    })
    oartialResultRdd
  }

  /**
   * Merge partial results to generate final training result that contains naive bayes model
   *
   * @param context DAAL Context
   * @param rdd RDD of partial results
   * @return Final result of algorithm
   */
  override def mergePartialResults(context: DaalContext, rdd: RDD[TrainingPartialResult]): TrainingResult = {
    val naiveBayesTraining = new TrainingDistributedStep2Master(context, classOf[java.lang.Double],
      TrainingMethod.defaultDense, numClasses)

    /* Build and retrieve final naive bayes model */
    val partialModelArray = rdd.collect()
    partialModelArray.foreach { partialModel =>
      partialModel.unpack(context)
      naiveBayesTraining.input.add(TrainingDistributedInputId.partialModels, partialModel)
    }

    naiveBayesTraining.compute()
    val trainingResult = naiveBayesTraining.finalizeCompute()
    trainingResult
  }

  /**
   * Serialize trained model to byte array
   * @param trainedModel Trained model
   * @return Serialized model
   */
  private def serializeTrainedModel(trainedModel: Model): List[Byte] = {
    val serializedModel = Try(ModelSerializer.serializeNaiveBayesModel(trainedModel).toList) match {
      case Success(sModel) => sModel
      case Failure(ex) => {
        println(s"Unable to serialize DAAL Naive Bayes model : ${ex.getMessage}")
        throw new scala.RuntimeException(s"Unable to serialize DAAL Naive Bayes model : ${ex.getMessage}")
      }
    }
    serializedModel
  }
}
