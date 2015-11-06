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

package org.trustedanalytics.atk.engine.gc

// "admin" plugins to manually run garbage collection

import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, CommandPlugin, Invocation }
import com.typesafe.config.ConfigFactory

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
/**
 * Arguments used for a single execution of garbage collection
 */
case class GarbageCollectionDropStaleArgs(@ArgDoc("""Minimum age of entity staleness.  Defaults to server config.  As a string it supports units according to https://github.com/typesafehub/config/blob/master/HOCON.md#duration-format""") staleAge: Option[String] = None)
case class NoArgs(@ArgDoc("""Ignore this argument""") bogus: Long = 0) // todo: this is a bogus reference for now to fit current plugin framework and JSON support

/** Json conversion for arguments and return value case classes */
object GcJsonFormat {
  import DomainJsonProtocol._
  implicit val GarbageCollectionDropStaleArgsFormat = jsonFormat1(GarbageCollectionDropStaleArgs)
  implicit val NoArgsFormat = jsonFormat1(NoArgs)

  /**
   * Converts unit of duration using typesafe's SimpleConfig.parseDuration into milliseconds
   * just like the config
   * @param str string value to convert into milliseconds
   * @return milliseconds age range from the string object
   */
  def stringToMilliseconds(str: String): Long = {
    val config = ConfigFactory.parseString(s"""string_value = "$str" """)
    config.getDuration("string_value", TimeUnit.MILLISECONDS)
  }
}

import GcJsonFormat._
import DomainJsonProtocol._

/**
 * Plugin that executes a single instance of garbage collection to drop all stale entities with user-specified age
 */
class GarbageCollectionDropStalePlugin extends CommandPlugin[GarbageCollectionDropStaleArgs, UnitReturn] {

  override def name: String = "_admin:/_drop_stale"

  override def execute(arguments: GarbageCollectionDropStaleArgs)(implicit context: Invocation): UnitReturn = {
    val staleAge = arguments.staleAge match {
      case Some(age) => Some(stringToMilliseconds(age))
      case None => None
    }
    GarbageCollector.singleTimeExecutionDropStale(staleAge)
  }
}

/**
 * Plugin that executes a single instance of garbage collection to finalize all dropped entities
 */
class GarbageCollectionFinalizeDroppedPlugin extends CommandPlugin[NoArgs, UnitReturn] {

  override def name: String = "_admin:/_finalize_dropped"

  override def execute(arguments: NoArgs)(implicit context: Invocation): UnitReturn = {
    GarbageCollector.singleTimeExecutionFinalizeDropped()
  }
}
