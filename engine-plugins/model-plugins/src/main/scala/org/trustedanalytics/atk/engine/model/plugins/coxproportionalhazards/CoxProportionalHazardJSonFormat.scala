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
package org.trustedanalytics.atk.engine.model.plugins.coxproportionalhazards

import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._

/** Json conversion for arguments and return value case classes */
object CoxProportionalHazardJSonFormat {

  implicit val coxProportionalHazardTrainFormat = jsonFormat8(CoxProportionalHazardTrainArgs)
  implicit val coxProportionalHazardPredictFormat = jsonFormat1(CoxProportionalHazardsPredictArgs)
  implicit val coxProportionalHazardTrainReturn = jsonFormat2(CoxProportionalHazardTrainReturn)
}
