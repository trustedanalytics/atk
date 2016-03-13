/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.domain.jobcontext

import org.joda.time.format.DateTimeFormat
import org.trustedanalytics.atk.domain.HasId
import org.joda.time.DateTime
import spray.json.JsObject

// JobContext class to store the yarn job & application master details
case class JobContext(id: Long,
                      userId: Long,
                      yarnAppName: Option[String],
                      yarnAppId: Option[String],
                      clientId: String,
                      createdOn: DateTime,
                      modifiedOn: DateTime,
                      progress: Option[String] = None,
                      jobServerUri: Option[String] = None) extends HasId {

  def getYarnAppName: String = {
    yarnAppName.getOrElse(throw new IllegalStateException("yarnAppName was never set"))
  }
}
