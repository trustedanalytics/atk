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

package org.trustedanalytics.atk.domain.frame

import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }
import spray.json.JsValue

/**
 * Command for calculating the mode of a (possibly weighted) column.
 */
case class ColumnModeArgs(@ArgDoc("""Identifier for the input dataframe.""") frame: FrameReference,
                          @ArgDoc("""Name of the column supplying the data.""") dataColumn: String,
                          @ArgDoc("""Name of the column supplying the weights.
Default is all items have weight of 1.""") weightsColumn: Option[String],
                          @ArgDoc("""Maximum number of modes returned.
Default is 1.""") maxModesReturned: Option[Int]) {

  require(frame != null, "frame is required")
  require(dataColumn != null, "data column is required")
}

/**
 *  * Mode data for a dataframe column.
 *
 * If no weights are provided, all elements receive a uniform weight of 1.
 * If any element receives a weight that is NaN, infinite or <= 0, that element is thrown
 * out of the calculation.
 *
 * @param modes A mode is a data element of maximum net weight. A set of modes is returned.
 *             The empty set is returned when the sum of the weights is 0. If the number of modes is <= the parameter
 *             maxNumberOfModesReturned, then all modes of the data are returned.If the number of modes is
 *             > maxNumberOfModesReturned, then only the first maxNumberOfModesReturned many modes
 *             (per a canonical ordering) are returned.
 * @param weightOfMode Weight of the mode. (If no weight column is specified,
 *                     this is the number of appearances of the mode.)
 * @param totalWeight Total weight in the column. (If no weight column is specified, this is the number of entries
 *                    with finite, non-zero weight.)
 * @param modeCount The number of modes in the data.
 */
case class ColumnModeReturn(modes: JsValue, weightOfMode: Double, totalWeight: Double, modeCount: Long) {
}
