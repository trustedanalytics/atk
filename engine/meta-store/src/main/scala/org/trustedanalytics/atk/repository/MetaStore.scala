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

import org.trustedanalytics.atk.event.EventContext
import org.trustedanalytics.atk.domain.{ Status, User, UserTemplate }

/**
 * The MetaStore gives access to Repositories. Repositories are how you
 * modify and query underlying tables (frames, graphs, users, etc).
 */
trait MetaStore {
  type Session
  def withSession[T](name: String)(f: Session => T)(implicit evc: EventContext = EventContext.getCurrent()): T

  def withTransaction[T](name: String)(f: Session => T)(implicit evc: EventContext = EventContext.getCurrent()): T

  /** Repository for CRUD on 'status' table */
  def statusRepo: Repository[Session, Status, Status]

  /** Repository for CRUD on 'frame' table */
  //def frameRepo: Repository[Session, DataFrameTemplate, DataFrame]
  def frameRepo: FrameRepository[Session]

  /** Repository for CRUD on 'graph' table */
  def graphRepo: GraphRepository[Session]

  /** Repository for CRUD on 'command' table */
  def commandRepo: CommandRepository[Session]

  /** Repository for CRUD on 'model' table */
  def modelRepo: ModelRepository[Session]

  /** Repository for CRUD on 'user' table */
  def userRepo: Repository[Session, UserTemplate, User] with Queryable[Session, User]

  /** Repository for CRUD on 'gc' table */
  def gcRepo: GarbageCollectionRepository[Session]

  /** Repository for CRUD on 'gc_entry' table */
  def gcEntryRepo: GarbageCollectionEntryRepository[Session]

  /** Repository for Job Context */
  def jobContextRepo: JobContextRepository[Session]

  /** Create the underlying tables */
  def initializeSchema(): Unit

  /** Delete ALL of the underlying tables - useful for unit tests only */
  private[repository] def dropAllTables(): Unit
}
