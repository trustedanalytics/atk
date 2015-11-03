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
}

object Status {

  /**
   * Available to user
   */
  final val Active = Status(1, "ACTIVE", "Available to user", new DateTime, null)
  /**
   * No longer available to user
   */
  final val Dropped = Status(2, "DROPPED", "No longer available to user", new DateTime, null)
  /**
   * Not available and data has been deleted
   */
  final val Finalized = Status(3, "FINALIZED", "Not available and data has been deleted", new DateTime, null)

  implicit def toString(v: Status): String = v.name
  implicit def toLong(v: Status): Long = v.id
  implicit def fromLong(v: Long): Status = v match {
    case Active.id => Active
    case Dropped.id => Dropped
    case Finalized.id => Finalized
  }
}
