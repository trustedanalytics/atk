/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.engine.model.plugins.clustering

import org.apache.spark.mllib.clustering.GaussianMixtureModel

/**
 * Command for loading model data into existing model in the model database.
 * @param gmmModel Trained MLLib's GaussianMixtureModel object
 * @param observationColumns Handle to the observation columns of the data frame
 * @param columnScalings Handle to the weights for the observation columns of the data frame
 */
case class GMMData(gmmModel: GaussianMixtureModel, observationColumns: List[String], columnScalings: List[Double]) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(columnScalings != null && columnScalings.nonEmpty, "columnWeights must not be null nor empty")
  require(columnScalings.length == observationColumns.length, "number of elements in observationColumns and columnWeights needs to be the same")
  require(gmmModel != null, "gmmModel must not be null")
}
