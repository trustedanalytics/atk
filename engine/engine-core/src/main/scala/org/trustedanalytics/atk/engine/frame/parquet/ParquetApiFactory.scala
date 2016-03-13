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
package org.trustedanalytics.atk.engine.frame.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileStatus, Path }
import parquet.column.impl.ColumnReadStoreImpl
import parquet.column.{ ColumnReadStore, ColumnDescriptor }
import parquet.column.page.PageReadStore
import parquet.hadoop.{ Footer, ParquetFileReader }
import parquet.hadoop.metadata.BlockMetaData
import parquet.schema.MessageType
import scala.collection.JavaConverters._

/**
 * Creates instances of parquet library API objects.
 * This factory class is in existence to allow mocking during unit tests
 * @param configuration Hadoop Configuration object
 */
class ParquetApiFactory(val configuration: Configuration) {
  private val createdBy = "parquet-hadoop version 1.5.0"

  /**
   * Encapsulates the third party ParquetFileReader constructor
   * @param filePath Path to Parquet Hadoop file
   * @param blocks The list of blocks found in this file according to the ParquetFooter
   * @param columns List of columns from a parquet schema
   * @return A ParquetFileReader for said file
   */
  def newParquetFileReader(filePath: Path, blocks: java.util.List[BlockMetaData],
                           columns: java.util.List[ColumnDescriptor]): ParquetFileReader = {
    new ParquetFileReader(this.configuration, filePath, blocks, columns)
  }

  /**
   * Encapsulates the third party ColumnReadStoreImpl constructor
   * @param pageReadStore a readStore representing the current ParquetPage
   * @param schema A Parquet MessageType object representing the schema
   * @return
   */
  def newColumnReadStore(pageReadStore: PageReadStore, schema: MessageType): ColumnReadStore = {
    new ColumnReadStoreImpl(pageReadStore, new ParquetRecordGroupConverter(), schema, createdBy)
  }

  /**
   *  Encapsulate the third party readSummaryFile static method.
   * @param metaDataFileStatus  Hadoop FileStatus object for the parquet metadata summary file
   * @return A list of ParquetFooter objects for each file that the metadata summary relates to.
   */
  def getFooterList(metaDataFileStatus: FileStatus): List[Footer] = {
    ParquetFileReader.readSummaryFile(this.configuration, metaDataFileStatus).asScala.toList
  }
}
