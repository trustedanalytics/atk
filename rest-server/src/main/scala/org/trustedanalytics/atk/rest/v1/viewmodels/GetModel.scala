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

/**
 * The REST service response for single command in "GET ../models/id"
 *
 * @param id unique id auto-generated by the database
 * @param name the name of the model
 * @param atk_uri the atk_uri of the model
 * @param links hyperlinks to related URIs
 */

case class GetModel(id: Long, atk_uri: String, name: Option[String], links: List[RelLink], entityType: String, status: String) {
  require(id > 0, "id must be greater than zero")
  require(name != null, "name must not be null")
  require(links != null, "links must not be null")
  require(atk_uri != null, "ia_uri must not be null")
  require(entityType != null, "entityType must not be null")
  require(status != null, "status may not be null")
}
