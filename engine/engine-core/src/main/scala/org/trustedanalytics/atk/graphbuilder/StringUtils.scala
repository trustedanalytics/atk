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
package org.trustedanalytics.atk.graphbuilder

/**
 * Prefer org.apache.commons.lang3.StringUtils over writing your own methods below
 */
object StringUtils {

  /**
   * Call toString() on the supplied object, handling null safely
   * @param any object
   * @return null if called on null object, otherwise result of toString()
   */
  def nullSafeToString(any: Any): String = {
    if (any != null) any.toString()
    else null
  }
}
