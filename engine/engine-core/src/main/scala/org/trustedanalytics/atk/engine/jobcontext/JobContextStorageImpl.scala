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
package org.trustedanalytics.atk.engine.jobcontext

import org.trustedanalytics.atk.NotFoundException
import org.trustedanalytics.atk.domain._
import org.trustedanalytics.atk.domain.command
import org.trustedanalytics.atk.domain.command.Command
import org.trustedanalytics.atk.domain.command.CommandTemplate
import org.trustedanalytics.atk.domain.jobcontext.{ JobContextTemplate, JobContext }
import org.trustedanalytics.atk.engine._
import org.trustedanalytics.atk.event.EventContext
import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.repository.SlickMetaStoreComponent

import scala.util.{ Success, Failure, Try }
import spray.json.JsObject
import org.trustedanalytics.atk.domain.command.{ CommandTemplate, Command }
import org.trustedanalytics.atk.repository.SlickMetaStoreComponent
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }

/**
 * Methods for modifying command records stored in Meta-Store
 */
class JobContextStorageImpl(val metaStore: SlickMetaStoreComponent#SlickMetaStore) extends JobContextStorage with EventLogging {
  val repo = metaStore.jobContextRepo

  /**
   * Generally it is better to call expectCommand() since commands don't get deleted,
   * so if you have an id it should be valid
   */
  override def lookup(id: Long): Option[JobContext] =
    metaStore.withSession("se.command.lookup") {
      implicit session =>
        repo.lookup(id)
    }

  override def lookupByName(appName: String): Option[JobContext] = metaStore.withSession("se.jobcontext.lookup") {
    implicit session =>
      repo.lookupByName(Some(appName))
  }

  override def create(createReq: JobContextTemplate): JobContext =
    metaStore.withSession("se.command.create") {
      implicit session =>

        val created = repo.insert(createReq)
        repo.lookup(created.get.id).getOrElse(throw new Exception("JobContext not found immediately after creation"))
    }

  override def scan(offset: Int, count: Int): Seq[JobContext] = metaStore.withSession("se.jobcontext.getJobContexts") {
    implicit session =>
      repo.scan(offset, count).sortBy(c => c.id) //TODO: Can't seem to get db to produce sorted results.
  }

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param updatedProgress Updated Progress of the command
   */
  override def updateProgress(id: Long, updatedProgress: String): Unit = metaStore.withSession("se.jobcontext.getJobContexts") {
    implicit session =>
      val jobContext = repo.lookup(id)
      val updatedJobContext = jobContext.get.copy(progress = Some(updatedProgress))
      repo.update(updatedJobContext)
  }

  override def updateJobServerUri(id: Long, jobServerUri: String): Unit = metaStore.withSession("se.jobcontext.getJobContexts") {
    implicit session =>
      val jobContext = repo.lookup(id)
      val updatedJobContext = jobContext.get.copy(jobServerUri = Some(jobServerUri))
      repo.update(updatedJobContext)
  }

}

