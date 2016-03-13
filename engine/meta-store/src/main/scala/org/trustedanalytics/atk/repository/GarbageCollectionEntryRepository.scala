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

import org.trustedanalytics.atk.domain.gc.{ GarbageCollection, GarbageCollectionEntry, GarbageCollectionEntryTemplate }

import scala.util.Try

/**
 * Repository for garbage collection entries
 */
trait GarbageCollectionEntryRepository[Session]
    extends Repository[Session, GarbageCollectionEntryTemplate, GarbageCollectionEntry] {

  /**
   * Return all the models
   * @param session current session
   * @return all the models
   */
  def scanAll()(implicit session: Session): Seq[GarbageCollectionEntry]

  def updateEndTime(entity: GarbageCollectionEntry)(implicit session: Session): Try[GarbageCollectionEntry]

}
