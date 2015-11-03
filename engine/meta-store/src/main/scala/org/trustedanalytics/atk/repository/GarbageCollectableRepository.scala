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


package org.trustedanalytics.atk.repository

import org.trustedanalytics.atk.domain.HasId
import org.trustedanalytics.atk.domain.gc.GarbageCollection
import org.joda.time.DateTime

import scala.util.Try

trait GarbageCollectableRepository[Session, Entity <: HasId] extends ReadRepository[Session, Entity] {
  /**
   * Return a list of Active entities which meet "stale" criteria, meaning GC can drop them
   * @param age milliseconds that must have passed since the last access to the entity for it to be considered "stale"
   * @param session current session
   */
  def getStaleEntities(age: Long)(implicit session: Session): Seq[Entity]

  /** update entity as having its data deleted */
  def finalizeEntity(entity: Entity)(implicit session: Session): Try[Entity]

  /** update the last read data of an entity */
  def updateLastReadDate(entity: Entity)(implicit session: Session): Try[Entity]
}
