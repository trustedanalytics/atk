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


package org.trustedanalytics.atk.rest.v1.viewmodels

import spray.json.JsValue
import org.trustedanalytics.atk.domain.schema.Schema

/**
 * A value that will be inserted into the result section of a GetQuery object
 *
 * @param data Actual requested Values
 * @param page Partition data came from
 * @param totalPages Total partitions for the data source
 * @param schema schema to describe the data returned by query
 */
case class GetQueryPage(data: Option[List[JsValue]], page: Option[Long], totalPages: Option[Long], schema: Option[Schema])
