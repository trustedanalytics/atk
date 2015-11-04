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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }
import spray.json.JsValue

/**
 * Command for calculating the median of a (possibly weighted) dataframe column.
 */
case class ColumnMedianArgs(frame: FrameReference,
                            @ArgDoc("""The column whose median is to be calculated.""") dataColumn: String,
                            @ArgDoc("""The column that provides weights (frequencies)
for the median calculation.
Must contain numerical data.
Default is all items have a weight of 1.""") weightsColumn: Option[String]) {

  require(frame != null, "frame is required")
  require(dataColumn != null, "data column is required")
}

/**
 * The median value of the (possibly weighted) column. None when the sum of the weights is 0.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 *
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 * @param value The median. None if the net weight of the column is 0.
 */
case class ColumnMedianReturn(value: JsValue)
