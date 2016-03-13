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
package org.trustedanalytics.atk.domain.datacatalog

import org.apache.commons.lang3.StringUtils
import scala.util.Random

/**
 * Response class for the data catalog entries
 */
case class TapDataCatalogResponse(id: String,
                                  size: Int,
                                  title: String,
                                  dataSample: String,
                                  recordCount: Int,
                                  isPublic: Boolean,
                                  targetUri: String,
                                  orgUUID: String,
                                  category: String,
                                  format: String,
                                  creationTime: String,
                                  sourceUri: String)

/*
 * Request class to add an entry to data catalog
 */
case class TapDataCatalogPutRequest(title: String,
                                    category: String,
                                    recordCount: Int,
                                    sourceUri: String,
                                    size: Int,
                                    orgUUID: String,
                                    targetUri: String,
                                    format: String,
                                    dataSample: String,
                                    isPublic: Boolean)

/*
 * Catalog Metadata class being returned by plugins. If any plugin returns an object of type CatalogMetadata,
 * python client will send a publish request to add the entry to data catalog
 */
case class ExportMetadata(targetUri: String,
                          category: String,
                          format: String,
                          sourceUri: String,
                          recordCount: Long,
                          dataSample: String,
                          title: String,
                          size: Long,
                          isPublic: Boolean)

// Auxiliary constructors for case class Export Metadata
object ExportMetadata {

  def apply(targetUri: String, category: String, format: String, size: Long, title: String): ExportMetadata =
    ExportMetadata(targetUri, category, format, targetUri,
      0, StringUtils.EMPTY, title, size, false)

  def apply(targetUri: String, category: String, format: String, recordCount: Option[Long], dataSample: String, size: Long, title: Option[String] = None): ExportMetadata =
    ExportMetadata(targetUri, category, format, targetUri,
      recordCount.getOrElse(0L), dataSample, title.getOrElse(Random.alphanumeric.slice(0, 16).mkString), size, false)
}

/*
 Converts a catalog metadata object (which is handled by ATK) to an input metadata entry for the data catalog service
 */
object ConvertExportMetadataToInputMetadataEntry {
  def convert(c: ExportMetadata, orgUUID: String) = {
    TapDataCatalogPutRequest(c.title, c.category, c.recordCount.toInt, c.sourceUri, c.size.toInt, orgUUID,
      c.targetUri, c.format, c.dataSample, c.isPublic)
  }

}