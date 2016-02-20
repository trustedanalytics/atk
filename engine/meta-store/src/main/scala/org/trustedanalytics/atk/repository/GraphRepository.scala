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

import org.trustedanalytics.atk.domain.frame.FrameEntity
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, GraphTemplate }

import scala.util.Try

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

  /** update a graph entity as Dropped */
  def dropGraph(graph: GraphEntity)(implicit session: Session): Try[GraphEntity]

  /** gets sequence of all graphs with status Dropped */
  def droppedGraphs(implicit session: Session): Seq[GraphEntity]
}
