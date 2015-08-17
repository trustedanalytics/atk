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

/**
 * Repository interface for read-only operations (basic look-ups) for a single table.
 *
 * @tparam Entity type of entity
 */
trait ReadRepository[Session, Entity <: HasId] {

  def defaultScanCount: Int = 20 //TODO: move to config

  /**
   * Find by Primary Key
   */
  def lookup(id: Long)(implicit session: Session): Option[Entity]

  /**
   * Get a 'page' of rows
   *
   * @param offset for pagination, start at '0' and increment
   * @param count number of rows to return
   */
  def scan(offset: Int = 0, count: Int = defaultScanCount)(implicit session: Session): Seq[Entity]
}
