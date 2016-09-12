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

import com.cloudera.sparkts.models.{ ARIMAXModel => MAXModel }

/**
 * Command for loading model data into existing model in the model database.
 * @param maxModel Trained MAXModel
 * @param xColumns Name of the column that contain exogenous variables
 */
case class MAXData(maxModel: MAXModel, xColumns: List[String]) {
  require(maxModel != null, "maxModel must not be null")
  require(xColumns != null && xColumns.nonEmpty, "xColumns list must not be null or empty")
}
