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

package org.trustedanalytics.atk.rest.v1.viewmodels

import spray.json.JsObject

/**
 * Generic JSON message for commands
 * @param name the name of the operation
 * @param arguments arguments for the operation
 */
case class JsonTransform(name: String, arguments: Option[JsObject]) {
  require(name != null, "Name is required")
}
