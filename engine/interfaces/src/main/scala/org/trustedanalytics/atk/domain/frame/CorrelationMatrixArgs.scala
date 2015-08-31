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

/**
 * Input arguments class for correlation matrix
 */
case class CorrelationMatrixArgs(@ArgDoc("""<TBD>""") frame: FrameReference,
                                 @ArgDoc("""The names of the columns from
which to compute the matrix.""") dataColumnNames: List[String],
                                 @ArgDoc("""The name for the returned
matrix Frame.""") matrixName: Option[String] = None) {
  require(frame != null, "frame is required")
  require(dataColumnNames.size >= 2, "two or more data columns are required")
  require(!dataColumnNames.contains(null), "data columns names cannot be null")
  require(dataColumnNames.forall(!_.equals("")), "data columns names cannot be empty")
}
