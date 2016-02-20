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

import org.trustedanalytics.atk.domain.gc.{ GarbageCollection, GarbageCollectionTemplate }

import scala.util.Try

/**
 * Repository for models
 */
trait GarbageCollectionRepository[Session]
    extends Repository[Session, GarbageCollectionTemplate, GarbageCollection] {

  /**
   * Return all the models
   * @param session current session
   * @return all the garbage Collections
   */
  def scanAll()(implicit session: Session): Seq[GarbageCollection]

  /**
   * Return all unended Garbage Collection Executions
   * @param session current session
   * @return all open gc entities
   */
  def getCurrentExecutions()(implicit session: Session): Seq[GarbageCollection]

  def updateEndTime(entity: GarbageCollection)(implicit session: Session): Try[GarbageCollection]

}
