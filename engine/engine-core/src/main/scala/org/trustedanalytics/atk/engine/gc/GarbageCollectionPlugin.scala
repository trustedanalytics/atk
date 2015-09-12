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

package org.trustedanalytics.atk.engine.gc

import java.util.concurrent.TimeUnit

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.domain.DomainJsonProtocol
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, CommandPlugin, Invocation, PluginDoc }
import org.trustedanalytics.atk.engine.{ ArgDocAnnotation, EngineConfig }
import com.typesafe.config.ConfigFactory

// Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
/**
 * Arguments used for a single execution of garbage collection
 */
case class GarbageCollectionArgs(@ArgDoc("""Minimum age of entity staleness.  Defaults to server config.  As a string it supports units according to https://github.com/typesafehub/config/blob/master/HOCON.md#duration-format""") staleAge: Option[String] = None)

/** Json conversion for arguments and return value case classes */
object GcJsonFormat {
  import DomainJsonProtocol._
  implicit val GarbageCollectionArgsFormat = jsonFormat1(GarbageCollectionArgs)
}

import GcJsonFormat._
import DomainJsonProtocol._

/**
 * Plugin that executes a single instance of garbage collection with user timespans specified at runtime
 */
class GarbageCollectionPlugin extends CommandPlugin[GarbageCollectionArgs, UnitReturn] {
  /**
   * The name of the command, e.g. graphs/ml/loopy_belief_propagation
   *
   *  This method does not execute against a specific object but is instead a static method
   *  Will be called against an explicitly written method
   *
   */
  override def name: String = "_admin:/_explicit_garbage_collection"

  /**
   * Execute a single instance of garbage collection
   */
  override def execute(arguments: GarbageCollectionArgs)(implicit context: Invocation): UnitReturn = {
    val staleAge = arguments.staleAge match {
      case Some(age) => Some(stringToMilliseconds(age))
      case None => None
    }
    GarbageCollector.singleTimeExecution(staleAge)
  }

  /**
   * Utilize the typesafe SimpleConfig.parseDuration method to allow this plugin to use the typesafe duration format
   * just like the config
   * @param str string value to convert into milliseconds
   * @return milliseconds age range from the string object
   */
  def stringToMilliseconds(str: String): Long = {
    val config = ConfigFactory.parseString(s"""string_value = "$str" """)
    config.getDuration("string_value", TimeUnit.MILLISECONDS)
  }
}
