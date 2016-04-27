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
package org.trustedanalytics.atk.engine.daal.plugins.regression.linear

/**
 * DAAL linear regression model
 *
 * @param serializedModel Serialized linear regression model
 * @param observationColumns List of column(s) storing the observations
 * @param valueColumn Column name containing the value for each observation
 * @param weights Weights of the trained model
 * @param intercept Intercept of the trained model
 */
case class DaalLinearRegressionModelData(serializedModel: List[Byte],
                                         observationColumns: List[String],
                                         valueColumn: String,
                                         weights: Array[Double],
                                         intercept: Double)

/**
 * JSON serialization for model
 */
object DaalLinearRegressionModelFormat {
  import org.trustedanalytics.atk.domain.DomainJsonProtocol._
  implicit val lrModelDataFormat = jsonFormat5(DaalLinearRegressionModelData)
}
