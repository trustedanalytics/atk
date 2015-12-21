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

package org.trustedanalytics.atk.engine

import org.trustedanalytics.atk.NotFoundException
import org.trustedanalytics.atk.domain.jobcontext.{ JobContextTemplate, JobContext }

trait JobContextStorage {

  /**
   * Generally it is better to call expectCommand() since commands don't get deleted,
   * so if you have an id it should be valid
   */
  def lookup(id: Long): Option[JobContext]

  def lookupByName(appName: String): Option[JobContext]

  /** Look-up a Command expecting it exists, throw Exception otherwise */
  def expectCommand(id: Long): JobContext = {
    lookup(id).getOrElse(throw new NotFoundException("JobContext", id))
  }

  /**
   * Add a new command to the meta store
   */
  def create(commandTemplate: JobContextTemplate): JobContext

  def scan(offset: Int, count: Int): Seq[JobContext]

  /**
   * update command info regarding progress of jobs initiated by this command
   * @param id command id
   * @param progressInfo list of progress for the jobs initiated by this command
   */
  def updateProgress(id: Long, progressInfo: String): Unit

  def updateJobServerUri(id: Long, jobServerUri: String): Unit

}

