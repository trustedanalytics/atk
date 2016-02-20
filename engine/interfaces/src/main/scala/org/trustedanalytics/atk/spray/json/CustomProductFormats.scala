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
package org.trustedanalytics.atk.spray.json

import spray.json.{ StandardFormats, ProductFormats }
import org.apache.commons.lang3.StringUtils

/**
 * Override the default behavior in ProductFormats to convert case class property names
 * to lower_case_with_underscores names in JSON.
 */
trait CustomProductFormats extends ProductFormats {
  // StandardFormats is required by ProductFormats
  this: StandardFormats =>

  /**
   * Override the default behavior in ProductFormats to convert case class property names
   * to lower_case_with_underscores names in JSON.
   */
  override protected def extractFieldNames(classManifest: ClassManifest[_]): Array[String] = {
    super.extractFieldNames(classManifest)
      .map(name => JsonPropertyNameConverter.camelCaseToUnderscores(name))
  }

}
