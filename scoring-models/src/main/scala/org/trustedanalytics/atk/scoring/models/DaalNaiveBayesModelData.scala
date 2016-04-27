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

import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext

/**
 * DAAL Naive Bayes model
 *
 * @param serializedModel Serialized Naive Bayes model
 * @param observationColumns List of column(s) storing the observations
 * @param labelColumn Column name containing the label
 * @param numClasses Number of classes
 * @param lambdaParameter Additive smoothing parameter
 * @param classPrior Optional prior probabilities of classes
 */
case class DaalNaiveBayesModelData(serializedModel: List[Byte],
                                   observationColumns: List[String],
                                   labelColumn: String,
                                   numClasses: Int,
                                   lambdaParameter: Double,
                                   classPrior: Option[Array[Double]] = None)

/**
 * Helper methods for getting DAAL Naive Bayes parameters
 */
object DaalNaiveBayesParameters {

  /**
   * Create DAAL numeric table with additive smoothing parameter
   *
   * @param context DAAL context
   * @param lambda Additive smoothing parameter
   * @param featureLength Feature length
   * @return Numeric table with additive smooting parameter
   */
  def getAlphaParameter(context: DaalContext, lambda: Double, featureLength: Int): HomogenNumericTable = {
    val alphaParameters = Array.fill[Double](featureLength)(lambda)
    new HomogenNumericTable(context, alphaParameters, alphaParameters.length, 1L)
  }

  /**
   * Create numeric table with class priors
   *
   * @param context DAAL context
   * @param classPrior Class priors
   * @return Numeric table with class priors
   */
  def getClassPriorParameter(context: DaalContext, classPrior: Array[Double]): HomogenNumericTable = {
    new HomogenNumericTable(context, classPrior, classPrior.length, 1L)
  }
}
