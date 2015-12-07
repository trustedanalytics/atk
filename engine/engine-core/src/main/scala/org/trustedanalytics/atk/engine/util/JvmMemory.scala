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

package org.trustedanalytics.atk.engine.util

import java.text.NumberFormat
import java.util.Locale

object JvmMemory {

  /**
   * Get a formatting String description of current JVM memory
   */
  def memory: String = {
    val formatter = NumberFormat.getInstance(Locale.US)
    "freeMemory=" + formatter.format(Runtime.getRuntime.freeMemory()) +
      ", totalMemory=" + formatter.format(Runtime.getRuntime.totalMemory()) +
      ", maxMemory=" + formatter.format(Runtime.getRuntime.maxMemory())
  }
}
