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
package org.trustedanalytics.atk.plugins.export.csv

import org.trustedanalytics.atk.domain.datacatalog.ExportMetadata

//TODO: the JSON serialization issue needs to be fixed such that we can return List[ExportMetadata] instead of List[String]
/**
 * Metadata class being returned by ExportGraphCsvPlugin.
 * @param metadata
 */
case class GraphExportMetadata(metadata: List[String])
