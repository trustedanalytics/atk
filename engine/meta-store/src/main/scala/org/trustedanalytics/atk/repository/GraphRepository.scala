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

import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphTemplate }

/**
 * Repository for graphs
 */
trait GraphRepository[Session] extends Repository[Session, GraphTemplate, GraphEntity] with NameableRepository[Session, GraphEntity] with GarbageCollectableRepository[Session, GraphEntity] {
  /**
   * Return all the graphs
   * @param session current session
   * @return all the graphs
   */
  def scanAll()(implicit session: Session): Seq[GraphEntity]

  def incrementIdCounter(id: Long, idCounter: Long)(implicit session: Session): Unit

  /**
   * Returns the liveness of the graph. If it is named or has named frames then it is a live graph.
   * @param id id of graph in question
   * @return true if graph is live, false if it is not
   */
  def isLive(id: GraphEntity)(implicit session: Session): Boolean

}
