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

package org.trustedanalytics.atk.rest

/**
 * Utility method for getting ids out of URL's
 */
object UrlParser {

  private val frameIdRegex = "/frames/(\\d+)".r

  /**
   * Get the frameId out of a URL in the format "../frames/id"
   * @return unique id
   */
  def getFrameId(url: String): Option[Long] = {
    val id = frameIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

  private val graphIdRegex = "/graphs/(\\d+)".r

  /**
   * Get the graphId out of a URL in the format "../graphs/id"
   * @return unique id
   */
  def getGraphId(url: String): Option[Long] = {
    val id = graphIdRegex.findFirstMatchIn(url).map(m => m.group(1))
    id.map(s => s.toLong)
  }

}
