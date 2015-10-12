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

package org.trustedanalytics.atk.spray.json

import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils

/**
 * Convert CamelCase String to lower_case_with_underscores.
 */
object JsonPropertyNameConverter {

  /**
   * Convert CamelCase String to lower_case_with_underscores.
   */
  def camelCaseToUnderscores(camelCase: String): String = {
    if (camelCase == null) {
      return null
    }
    // This is only called when setting up JSON formats for case classes (NOT every time
    // you call toJSON) so it doesn't need to be particularly efficient.
    val parts = StringUtils.splitByCharacterTypeCamelCase(camelCase.trim)
    val mixedCaseWithUnderscores = StringUtils.join(parts.asInstanceOf[Array[AnyRef]], "_")
    val lower = mixedCaseWithUnderscores.toLowerCase
    // remove extra underscores (these might be added if the string already had underscores in it)
    replacePattern(lower, "[_]+", "_")
  }

  /**
   * Same as org.apache.commons.lang3.StringUtils.replacePattern
   *
   * Copied here because an earlier version of commons-lang3 was getting picked up when running Giraph.
   */
  private def replacePattern(source: String, regex: String, replacement: String): String = {
    Pattern.compile(regex, Pattern.DOTALL).matcher(source).replaceAll(replacement)
  }
}
