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

import org.apache.spark.mllib.regression.LinearRegressionModel

/**
 * Command for loading model data into existing model in the model database.
 * @param linRegModel Trained MLLib's LinearRegressionModel object
 * @param observationColumns Handle to the obsergvation columns of the data frame
 */
case class LinearRegressionWithSGDData(linRegModel: LinearRegressionModel, observationColumns: List[String]) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(linRegModel != null, "linRegModel must not be null")
}

/**
 * Results of Linear Regression Train plugin
 * @param observationColumns List of column names on which the model was trained
 * @param labelColumn Column name containing the true value of the observation
 * @param weightsVector An array of weights of the trained model
 * @param intercept Intercept value of the trained model
 */
case class LinearRegressionWithSGDTrainReturn(observationColumns: List[String], labelColumn: String, weightsVector: Array[Double], intercept: Double)
