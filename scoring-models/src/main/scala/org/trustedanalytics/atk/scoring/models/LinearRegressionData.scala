
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

import org.apache.spark.ml.regression.LinearRegressionModel

/**
 * Linear Regression data object
 * @param model The trained LinearRegression Model
 * @param observationColumns Frame's column(s) storing the observations
 * @param labelColumn Frame's column storing the label
 */
case class LinearRegressionData(model: LinearRegressionModel, observationColumns: List[String], labelColumn: String) {
  require(observationColumns != null && observationColumns.nonEmpty, "observationColumns must not be null nor empty")
  require(model != null, "linRegModel must not be null")
  require(labelColumn != null && labelColumn != "", "labelColumn must not be null or empty")
}
