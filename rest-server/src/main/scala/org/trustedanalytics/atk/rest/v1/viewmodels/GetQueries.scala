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
package org.trustedanalytics.atk.rest.v1.viewmodels

/**
 * The REST service response for a single command in the list provided by "GET ../queries".
 *
 * @param id unique id auto-generated by the database
 * @param name the name of the query to be performed. This name is purely for descriptive purposes.
 * @param url the URI for 'this' query in terms of the REST API
 */
case class GetQueries(id: Long, name: String, url: String) {
  require(id > 0)
  require(name != null)
  require(url != null)
}
