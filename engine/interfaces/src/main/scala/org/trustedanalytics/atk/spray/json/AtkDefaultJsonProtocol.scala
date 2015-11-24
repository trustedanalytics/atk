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

package org.trustedanalytics.atk.spray.json

import spray.json._

/**
 * Our JsonProtocol is similar to Spray's DefaultJsonProtocol
 * except we handle ProductFormats differently.
 */
trait AtkDefaultJsonProtocol extends BasicFormats
  with StandardFormats
  with CollectionFormats
  with CustomProductFormats
  with AdditionalFormats

/**
 * Our JsonProtocol is similar to Spray's DefaultJsonProtocol
 * except we handle ProductFormats differently.
 */
object AtkDefaultJsonProtocol extends AtkDefaultJsonProtocol
