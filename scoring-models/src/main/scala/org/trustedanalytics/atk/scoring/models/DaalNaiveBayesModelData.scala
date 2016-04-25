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

/**
 * DAAL Naive Bayes model
 *
 * @param serializedModel Serialized Naive Bayes model
 * @param observationColumns List of column(s) storing the observations
 * @param labelColumn Column name containing the label
 * @param numClasses Number of classes
 * @param alpha Imagined occurrences of features
 */
case class DaalNaiveBayesModelData(serializedModel: List[Byte],
                                   observationColumns: List[String],
                                   labelColumn: String,
                                   numClasses: Int,
                                   alpha: Double)
