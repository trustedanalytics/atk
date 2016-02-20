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
package org.trustedanalytics.atk.rest.v1.viewmodels

import org.apache.commons.lang3.StringUtils

/**
 * Links with relations
 * @param rel the relationship of the link to the current document
 * @param uri the link
 * @param method the HTTP method that should be used to retrieve the link
 */
case class RelLink(rel: String, uri: String, method: String) {
  require(rel != null, "rel must not be null")
  require(uri != null, "uri must not be null")
  require(method != null, "method must not be null")
  require(Rel.AllowedMethods.contains(method), "method must be one of " + Rel.AllowedMethods.mkString(", "))
}

/**
 * Convenience methods for constructing RelLinks
 */
object Rel {

  val AllowedMethods = List("GET", "PUT", "POST", "HEAD", "DELETE", "OPTIONS")

  /**
   * Self links
   */
  def self(uri: String) = RelLink(rel = "self", uri = uri, method = "GET")
}
