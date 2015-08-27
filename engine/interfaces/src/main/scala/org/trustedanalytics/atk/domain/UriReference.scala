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

/**
 * Things that can be referenced with a simple URI of the form ia://entity/id.
 */
trait UriReference {

  /** The entity id */
  def id: Long

  /** The entity name e.g. "frames", "graphs", ... */
  def entityCollectionName: String

  /** The full URI */
  final def uri: String = {
    s"$entityCollectionName/$id"
  }

  override def hashCode(): Int = uri.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case x: UriReference => this.uri == x.uri
    case _ => false
  }
}

object UriReference {

  lazy val pattern = """^(.+)/(\d+)$""".r

  def fromString[T <: UriReference](s: String, constructor: (Long => T)): T = {
    s match {
      case pattern(entityCollectionName, id) =>
        val ref = constructor(id.toLong)
        if (ref.entityCollectionName != entityCollectionName) {
          throw new RuntimeException(s"Internal Error: Invalid UriReference class applied to uri '$s'.  Expected '${ref.entityCollectionName}', got '$entityCollectionName'")
        }
        ref
      case _ => throw new RuntimeException(s"Malformed uri '$s'")
    }
  }
}
