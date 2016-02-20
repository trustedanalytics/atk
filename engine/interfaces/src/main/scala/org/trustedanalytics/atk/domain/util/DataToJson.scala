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
package org.trustedanalytics.atk.domain.util

import spray.json._

object DataToJson {

  /**
   * Convert an Iterable of Any to a List of JsValue. Required due to how spray-json handles AnyVals
   * @param data iterable to return in response
   * @return JSON friendly version of data
   */
  def apply(data: Iterable[Array[Any]]): List[JsValue] = {
    import org.trustedanalytics.atk.domain.DomainJsonProtocol._
    data.map(row => row.map {
      case null => JsNull
      case a => a.toJson
    }.toJson).toList
  }

}
