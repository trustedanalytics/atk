/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/

package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.HasId
import org.trustedanalytics.atk.domain.gc.GarbageCollection
import org.joda.time.DateTime

import scala.util.Try

trait GarbageCollectableRepository[Session, Entity <: HasId] extends ReadRepository[Session, Entity] {
  /**
   * Return a list of entities ready to delete
   * @param age the length of time in milliseconds for the newest possible record to be deleted
   * @param session current session
   */
  def listReadyForDeletion(age: Long)(implicit session: Session): Seq[Entity]

  /**
   * update and mark an entity as having it's data deleted
   * @param entity entity to be deleted
   * @param session the user session
   * @return the entity
   */
  def updateDataDeleted(entity: Entity)(implicit session: Session): Try[Entity]

  /**
   * update the last read data of an entity if it has been marked as deleted change it's status
   * @param entity entity to be updated
   * @param session the user session
   */
  def updateLastReadDate(entity: Entity)(implicit session: Session): Try[Entity]

  /**
   * update and mark an entity as being ready to delete in the next garbage collection execution
   * @param entity
   * @param session
   * @return
   */
  def updateReadyToDelete(entity: Entity)(implicit session: Session): Try[Entity]

}
