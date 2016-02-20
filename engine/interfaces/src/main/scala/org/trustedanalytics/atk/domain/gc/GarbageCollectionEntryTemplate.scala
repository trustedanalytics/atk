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
package org.trustedanalytics.atk.domain.gc

import org.joda.time.DateTime

/**
 * Arguments needed for creating a new instance of a GarbageCollectionEntry
 * @param garbageCollectionId unique Id of the garbage collection
 * @param description description of the entity being garbage collected
 * @param startTime time deletion starts
 */
case class GarbageCollectionEntryTemplate(garbageCollectionId: Long,
                                          description: String,
                                          startTime: DateTime)
