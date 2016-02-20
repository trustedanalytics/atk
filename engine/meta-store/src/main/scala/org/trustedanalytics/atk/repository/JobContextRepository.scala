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
package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.User
import org.trustedanalytics.atk.domain.jobcontext.{ JobContext, JobContextTemplate }

trait JobContextRepository[Session] extends Repository[Session, JobContextTemplate, JobContext] {

  def lookupByYarnAppName(name: Option[String])(implicit session: Session): Option[JobContext]

  def lookupByClientId(user: User, clientId: String)(implicit session: Session): Option[JobContext]

  def lookupRecentlyActive(seconds: Int)(implicit session: Session): Seq[JobContext]

  def updateJobServerUri(id: Long, uri: String)(implicit session: Session): Unit

  def updateProgress(id: Long, progress: String)(implicit session: Session): Unit

  def updateYarnAppName(id: Long, yarnAppName: String)(implicit session: Session): Unit
}