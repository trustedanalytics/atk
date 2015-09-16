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

package org.trustedanalytics.atk.domain

import org.joda.time.DateTime

/**
 * Lifecycle Status for Graphs and Frames
 * @param id unique id in the database
 * @param name the short name, For example, INIT (building), ACTIVE, DELETED (undelete possible), DELETE_FINAL (no undelete), INCOMPLETE (failed construction)
 * @param description two or three sentence description of the status
 * @param createdOn date/time this record was created
 * @param modifiedOn date/time this record was last modified
 */
case class Status(id: Long, name: String, description: String, createdOn: DateTime, modifiedOn: DateTime) extends HasId {

  require(name != null, "name must not be null")

  /** Active and can be interacted with */
  def isActive: Boolean = id.equals(Status.Active)

  /** Deleted but can still be un-deleted, no action has yet been taken on disk */
  def isDeleted: Boolean = id.equals(Status.Deleted)

  /** Underlying storage has been reclaimed, no un-delete is possible */
  def isDeleteFinal: Boolean = id.equals(Status.Deleted_Final)
}

object Status {

  /**
   * Return the proper id for a read garbage collectible entity
   * @param id the status id before the read
   * @return proper status id for after the read
   */
  def getNewStatusForRead(id: Long): Long =
    if (id == Deleted)
      Active
    else
      id

  def getName(id: Long): String = {
    id match {
      case Active => "Active"
      case Deleted => "Deleted (scheduled may be undeleted by modifying or inspecting)"
      case Deleted_Final => "Deleted Final"
      case _ => "Unkown"
    }
  }

  /** Active and can be interacted with */
  final val Active: Long = 1

  /** User has marked as Deleted but can still be un-deleted, no action has yet been taken on disk */
  final val Deleted: Long = 2

  /** Underlying storage has been reclaimed, no un-delete is possible */
  final val Deleted_Final: Long = 3
}
