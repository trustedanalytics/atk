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
package org.trustedanalytics.atk.engine.model.plugins.collaborativefiltering

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object CollaborativeFilteringJsonFormat {

  implicit val cfTrainArgs = jsonFormat13(CollaborativeFilteringTrainArgs)
  implicit val cfScoreArgs = jsonFormat3(CollaborativeFilteringScoreArgs)
  implicit val cfPredictArgs = jsonFormat7(CollaborativeFilteringPredictArgs)
  implicit val cfRecommendArgs = jsonFormat4(CollaborativeFilteringRecommendArgs)
  implicit val cfRecommendSingleReturn = jsonFormat3(CollaborativeFilteringSingleRecommendReturn)
  implicit val cfRecommendReturn = jsonFormat1(CollaborativeFilteringRecommendReturn)
  implicit val cfFilterArgs = jsonFormat3(CollaborativeFilteringData)
}
