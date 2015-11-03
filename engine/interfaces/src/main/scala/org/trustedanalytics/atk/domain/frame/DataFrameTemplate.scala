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


package org.trustedanalytics.atk.domain.frame

case class DataFrameTemplate(name: Option[String], description: Option[String] = None) {
  require(name != null, "name must not be null")
  if (name.isDefined) {
    require(name.get.trim.length > 0, "if name is set it must not be empty or whitespace")
    FrameName.validate(name.get)
  }
  require(description != null, "description must not be null")
}
