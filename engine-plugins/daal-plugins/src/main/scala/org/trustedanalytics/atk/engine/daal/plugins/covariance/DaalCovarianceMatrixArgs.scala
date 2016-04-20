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
package org.trustedanalytics.atk.engine.daal.plugins.covariance

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.engine.plugin.ArgDoc

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object DaalCovarianceMatrixJsonFormat {
  implicit val daalCovarianceMatrixArgsFormat = jsonFormat3(DaalCovarianceMatrixArgs)
}

/**
 * Input arguments class for DAAL covariance matrix
 */
case class DaalCovarianceMatrixArgs(frame: FrameReference,
                                    @ArgDoc("""The names of the column from which to compute the matrix.
Names should refer to a single column of type vector, or two or more
columns of numeric scalars.""") dataColumnNames: List[String],
                                    @ArgDoc("""The name of the new
matrix.""") matrixName: Option[String] = None) {
  require(frame != null, "frame is required")
  require(!dataColumnNames.contains(null), "data columns names cannot be null")
  require(dataColumnNames.forall(!_.equals("")), "data columns names cannot be empty")
}
