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
package org.trustedanalytics.atk.engine.daal.plugins.kmeans

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** JSON conversion for arguments and return value case classes */
object DaalKMeansJsonFormat {
  implicit val daalKMeansArgsFormat = jsonFormat8(DaalKMeansTrainArgs)
  implicit val daalKmeansReturnArgsFormat = jsonFormat2(DaalKMeansTrainReturn)
  implicit val daalKmeansModelData = jsonFormat5(DaalKMeansModelData)
  implicit val daalKmeansPredictArgsFormat = jsonFormat4(DaalKMeansPredictArgs)
}