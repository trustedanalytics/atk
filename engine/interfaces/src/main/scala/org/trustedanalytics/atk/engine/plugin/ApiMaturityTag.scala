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
package org.trustedanalytics.atk.engine.plugin

/**
 * Enumeration of the API Tags with which operations may tagged.
 *
 * These tags get exposed in end-user documentation.
 */
object ApiMaturityTag extends Enumeration {
  type ApiMaturityTag = Value

  /**
   * API item is new, has not gone through QA
   */
  val Alpha = Value

  /**
   * API item has passed QA, but may have performance or stability issues.  The signature is also still subject to change.
   */
  val Beta = Value

  /**
   * API item is no longer in favor and is going away.
   */
  val Deprecated = Value

  /** Implicit conversion to an Option */
  implicit def toOption(apiMaturityTag: ApiMaturityTag.Value): Option[ApiMaturityTag.Value] = {
    Some(apiMaturityTag)
  }
}
