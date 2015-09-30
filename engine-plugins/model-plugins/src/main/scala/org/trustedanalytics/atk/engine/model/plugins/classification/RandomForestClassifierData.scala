/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.trustedanalytics.atk.engine.model.plugins.classification

import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.atk.plugins.MLLibJsonProtocol._

/**
 * Command for loading model data into existing model in the model database.
 * @param randomForestModel Trained MLLib's LinearRegressionModel object
 * @param observationColumns Handle to the observation columns of the data frame
 * @param numClasses Number of classes of the data
 */
case class RandomForestClassifierData(randomForestModel: RandomForestModel, observationColumns: List[String], numClasses: Int) {
  require(observationColumns != null && !observationColumns.isEmpty, "observationColumns must not be null nor empty")
  require(randomForestModel != null, "randomForestModel must not be null")
}
