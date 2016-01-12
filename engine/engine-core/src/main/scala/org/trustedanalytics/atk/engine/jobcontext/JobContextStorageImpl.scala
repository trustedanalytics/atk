/**
 * Copyright (c) 2015 Intel Corporation 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.atk.engine.jobcontext

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.trustedanalytics.atk.domain.User
import org.trustedanalytics.atk.domain.jobcontext.{ JobContext, JobContextTemplate }
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.repository.SlickMetaStoreComponent

class JobContextStorageImpl(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends EventLogging {
  val repo = metaStore.jobContextRepo

  def expectJobContext(id: Long): JobContext = {
    lookup(id).getOrElse(throw new RuntimeException(s"JobContext with id $id was NOT found"))
  }

  def lookup(id: Long): Option[JobContext] = metaStore.withSession("se.command.lookup") {
    implicit session =>
      repo.lookup(id)
  }

  def lookupOrCreate(user: User, clientId: String): JobContext = {
    lookupByClientId(user, clientId) match {
      case Some(jobContext) =>

        // refresh it
        expectJobContext(jobContext.id)
      case None => create(new JobContextTemplate(user.id, clientId))
    }
  }

  def lookupByYarnAppName(appName: String): Option[JobContext] = metaStore.withSession("se.jobcontext.lookup") {
    implicit session =>
      repo.lookupByYarnAppName(Some(appName))
  }

  def lookupByClientId(user: User, clientId: String): Option[JobContext] = metaStore.withSession("se.jobcontext.lookup") {
    implicit session =>
      repo.lookupByClientId(user, clientId)
  }

  def lookupRecentlyActive(seconds: Int): Seq[JobContext] = metaStore.withSession("se.jobcontext.lookupRecentlyActive") {
    implicit session =>
      repo.lookupRecentlyActive(seconds)
  }

  def create(createReq: JobContextTemplate): JobContext =
    metaStore.withSession("se.command.create") {
      implicit session =>

        val created = repo.insert(createReq)
        repo.lookup(created.get.id).getOrElse(throw new Exception("JobContext not found immediately after creation"))
    }

  def scan(offset: Int, count: Int): Seq[JobContext] = metaStore.withSession("se.jobcontext.getJobContexts") {
    implicit session =>
      repo.scan(offset, count).sortBy(c => c.id) //TODO: Can't seem to get db to produce sorted results.
  }

  def updateProgress(id: Long, updatedProgress: String): Unit = metaStore.withSession("se.jobcontext.updateProgress") {
    implicit session =>
      repo.updateProgress(id, updatedProgress)
  }

  def updateJobServerUri(id: Long, jobServerUri: String): Unit = metaStore.withSession("se.jobcontext.updateJobServer") {
    implicit session =>
      repo.updateJobServerUri(id, jobServerUri)
  }

  def assignYarnAppName(jobContext: JobContext): JobContext = {
    val yarnAppName = s"client${jobContext.id}-${jobContext.clientId}-" + DateTimeFormat.forPattern("yyyymmdd_kk:mm").print(new DateTime)
    updateYarnAppName(jobContext.id, yarnAppName)
    expectJobContext(jobContext.id)
  }

  def updateYarnAppName(id: Long, yarnAppName: String): Unit = metaStore.withSession("se.jobcontext.yarnAppName") {
    implicit session =>
      repo.updateYarnAppName(id, yarnAppName)
  }

}

