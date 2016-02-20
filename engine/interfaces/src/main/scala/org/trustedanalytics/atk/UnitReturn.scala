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
package org.trustedanalytics.atk

/**
 * Unit return type for plugin.
 *
 * Currently, we have to give some value, so this is how we are representing Unit
 */
case class UnitReturn()

object UnitReturn {

  // Implicit conversion so plugin authors can mark their execute methods as having UnitReturn type,
  // without explicitly creating an instance of UnitReturn
  implicit def unitReturn(any: Any): UnitReturn = UnitReturn()
}