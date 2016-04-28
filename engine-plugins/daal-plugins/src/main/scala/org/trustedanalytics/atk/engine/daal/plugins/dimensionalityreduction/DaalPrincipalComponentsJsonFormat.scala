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
package org.trustedanalytics.atk.engine.daal.plugins.dimensionalityreduction

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** JSON conversion for arguments and return value case classes */
object DaalPrincipalComponentsJsonFormat {
  implicit val daalSvdDataFormat = jsonFormat6(DaalSvdData)
  implicit val daalPcaTrainArgsFormat = jsonFormat5(DaalPrincipalComponentsTrainArgs)
  implicit val daalPcaTrainReturnFormat = jsonFormat6(DaalPrincipalComponentsTrainReturn)
}

