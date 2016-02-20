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
import org.trustedanalytics.atk.domain.command.{ Command, CommandTemplate }
import org.trustedanalytics.atk.domain.jobcontext.JobContext
import spray.json.JsObject
import scala.util.Try
import org.trustedanalytics.atk.engine.ProgressInfo

/**
 * Repository for command records
 */
trait CommandRepository[Session] extends Repository[Session, CommandTemplate, Command] {

  def lookup(jobContext: JobContext)(implicit session: Session): Seq[Command]

  def lookupNotComplete()(implicit session: Session): Seq[Command]

  def updateComplete(id: Long, complete: Boolean)(implicit session: Session): Try[Unit]

  def updateProgress(id: Long, progressInfo: List[ProgressInfo])(implicit session: Session): Try[Unit]

  def updateJobContextId(id: Long, jobContextId: Long)(implicit session: Session): Try[Unit]

  def updateResult(id: Long, result: JsObject)(implicit session: Session): Try[Unit]
}
