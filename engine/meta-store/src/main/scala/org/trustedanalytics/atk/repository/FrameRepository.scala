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

import org.trustedanalytics.atk.domain.UserPrincipal
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, DataFrameTemplate }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{ Schema, DataTypes }
import DataTypes.DataType

import scala.util.Try

trait FrameRepository[Session] extends Repository[Session, DataFrameTemplate, FrameEntity] with NameableRepository[Session, FrameEntity] with GarbageCollectableRepository[Session, FrameEntity] {

  def insert(frame: FrameEntity)(implicit session: Session): FrameEntity

  def updateRowCount(frame: FrameEntity, rowCount: Option[Long])(implicit session: Session): FrameEntity

  def updateSchema(frame: FrameEntity, schema: Schema)(implicit session: Session): FrameEntity

  /** Update the errorFrameId column */
  def updateErrorFrameId(frame: FrameEntity, errorFrameId: Option[Long])(implicit session: Session): FrameEntity

  /**
   * Return all the frames
   * @param session current session
   * @return all the dataframes
   */
  def scanAll()(implicit session: Session): Seq[FrameEntity]

  def lookupByGraphId(graphId: Long)(implicit session: Session): Seq[FrameEntity]

  /** updates a frame entity as Dropped */
  def dropFrame(frame: FrameEntity)(implicit session: Session): Try[FrameEntity]

  /** gets sequence of all frames with status Dropped */
  def droppedFrames(implicit session: Session): Seq[FrameEntity]

  /** determines if a frame is listed as an error frame of any other active frame */
  def isErrorFrame(frame: FrameEntity)(implicit session: Session): Boolean
}
