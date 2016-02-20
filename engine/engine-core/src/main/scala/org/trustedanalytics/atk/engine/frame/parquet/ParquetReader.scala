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

import java.nio.charset.Charset

import org.trustedanalytics.atk.engine.FileStorage
import org.apache.hadoop.fs.{ Path, FileSystem }
import parquet.column.{ ColumnReadStore, ColumnReader, ColumnDescriptor }
import parquet.column.page.PageReadStore
import parquet.hadoop.{ Footer, ParquetFileReader }
import parquet.hadoop.metadata.ParquetMetadata
import parquet.io.ParquetDecodingException
import parquet.schema.PrimitiveType.PrimitiveTypeName
import parquet.schema.MessageType
import scala.collection.JavaConverters._

import scala.collection.mutable.{ ArrayBuffer, ListBuffer }
import scala.collection.JavaConverters._
import org.trustedanalytics.atk.domain.schema.DataTypes

/**
 * A class that will read data from parquet files located at a specified path.
 * @param path path containing the parquet files
 * @param fileStorage fileStorage object containing locations for stored RDDs
 */
class ParquetReader(val path: Path, fileStorage: FileStorage, parquetApiFactory: ParquetApiFactory) {
  def this(path: Path, fileStorage: FileStorage) = this(path, fileStorage, new ParquetApiFactory(fileStorage.configuration))
  private[parquet] val utf8 = Charset.forName("UTF-8")
  private val decoder = utf8.newDecoder()
  private val ParquetFileOrderPattern = ".*part-r-(\\d*).*\\.parquet".r

  /**
   * Row count for the frame stored at this path
   */
  def rowCount(): Long = {
    val files: List[Footer] = getFiles()
    var result = 0L
    for {
      file <- files
      block <- file.getParquetMetadata.getBlocks.asScala
    } {
      result += block.getRowCount
    }
    result
  }

  def files(): List[Path] = {
    getFiles().map(footer => footer.getFile)
  }

  /**
   * take from this objects path data from parquet files as a list of arrays.
   * @param count total rows to be included
   * @param offset rows to be skipped before including rows
   * @param limit limit on number of rows to be included
   */
  def take(count: Long, offset: Long = 0, limit: Option[Long]): List[Array[Any]] = {
    val files: List[Footer] = getFiles()
    val capped = limit match {
      case None => count
      case Some(l) => Math.min(count, l)
    }
    var currentOffset: Long = 0
    var currentCount: Long = 0

    val result: ListBuffer[Array[Any]] = new ListBuffer[Array[Any]]
    for (
      file <- files if currentCount < capped
    ) {
      val metaData = file.getParquetMetadata
      val fileRowCount = getRowCountFromMetaData(metaData)

      if ((currentOffset + fileRowCount) > offset) {
        // Only process the file if it contains data that we need. 
        // If it does not skip and increase the offset by the file row count (derived from summary file).
        val schema = metaData.getFileMetaData.getSchema
        var reader: ParquetFileReader = null
        try {
          reader = this.parquetApiFactory.newParquetFileReader(file.getFile, metaData.getBlocks, schema.getColumns)
          var store: PageReadStore = reader.readNextRowGroup()
          while (store != null && currentCount < capped) {
            if ((currentOffset + store.getRowCount) > offset) {
              val start: Long = math.max(offset - currentOffset, 0)
              val amountToTake: Long = math.min(store.getRowCount - start, capped - currentCount)

              val crstore = this.parquetApiFactory.newColumnReadStore(store, schema)

              val pageReadStoreResults = takeFromPageReadStore(schema, crstore, amountToTake.toInt, start.toInt)
              result ++= pageReadStoreResults
              currentCount = currentCount + pageReadStoreResults.size
            }

            currentOffset = currentOffset + store.getRowCount.toInt
            store = reader.readNextRowGroup()
          }
        }
        finally {
          if (reader != null)
            reader.close()
        }
      }
      else {
        currentOffset = currentOffset + fileRowCount
      }
    }
    result.toList
  }

  /**
   * Read data from a single page
   * @param schema Parquet schema describing columns in this page
   * @param store ColumnReadStore for this page
   * @param count number of rows to take from page
   * @param offset number of rows to skip in this page
   */
  private[parquet] def takeFromPageReadStore(schema: MessageType, store: ColumnReadStore, count: Int, offset: Int): List[Array[Any]] = {
    val result = new ArrayBuffer[Array[Any]]()
    val columns = schema.getColumns.asScala
    val columnslength = columns.size

    columns.zipWithIndex.foreach {
      case (col: ColumnDescriptor, columnIndex: Int) => {
        // parquet uses nested types based on a definition level and a repetition level, as described by the
        // "Dremel Paper".  For a good explanation, see https://blog.twitter.com/2013/dremel-made-simple-with-parquet

        // Note: blbarker - the approach in this code is inadequate for general nesting solution, but it works
        // for primitives and arrays.  More complex types will require work in here to become a true parquet reader.

        val dMax = col.getMaxDefinitionLevel
        val creader = store.getColumnReader(col)

        if (offset > 0) {
          0.to(offset - 1).foreach(_ => {
            if (creader.getCurrentDefinitionLevel == dMax)
              creader.skip()
            creader.consume()
          })
        }

        // go through each triplet (def level, rep level, values) in the column chunk
        0.to(count - 1).foreach(chunkRelativeRowIndex => {

          if (result.size <= chunkRelativeRowIndex) {
            result += new Array[Any](columnslength)
          }

          val dlvl = creader.getCurrentDefinitionLevel
          val value = if (dlvl == dMax) {
            val v = getValue(creader, col.getType)
            creader.consume()
            if (col.getMaxRepetitionLevel > 0 && creader.getCurrentRepetitionLevel > 0) {
              // indicates nested type, like an array
              if (col.getType == PrimitiveTypeName.DOUBLE) {
                val array = new ArrayBuffer[Double]()
                array += DataTypes.toDouble(v)
                while (creader.getCurrentRepetitionLevel > 0) { // rep level == 0 indicates start of another group
                  array += creader.getDouble
                  creader.consume()
                }
                array
              }
              // only support arrays of doubles for now
              else {
                throw new ClassCastException(s"trustedanalytics parquet reader does not support repetition of type ${col.getType}")
              }
            }
            else {
              v // not nested, just use value
            }
          }
          else {
            creader.consume()
            null
          }
          result(chunkRelativeRowIndex)(columnIndex) = value
        })

      }
    }
    result.toList
  }

  /**
   * Compute the total number of rows in a file by summing the row counts of all blocks
   * @param metaData ParquetMetaData object for the corresponding file (may come from a summary file)
   * @return row count
   */
  private[parquet] def getRowCountFromMetaData(metaData: ParquetMetadata): Long = {
    metaData.getBlocks.asScala.map(b => b.getRowCount).sum
  }

  /**
   * Returns a sequence of Paths that will be evaluated for finding the required rows
   * @return  List of FileStatus objects corresponding to grouped parquet files
   */
  private[parquet] def getFiles(): List[Footer] = {
    def fileHasRows(metaData: ParquetMetadata): Boolean = {
      getRowCountFromMetaData(metaData) > 0
    }
    val fs: FileSystem = fileStorage.hdfs
    if (!fs.isDirectory(path)) {
      val metadata = ParquetFileReader.readFooter(fileStorage.configuration, fs.getFileStatus(path))
      List(new Footer(path, metadata))
    }
    else {
      def getFileNumber(path: Path): Int = {
        path.getName match {
          case ParquetFileOrderPattern(i) => i.toInt
          case _ => -1
        }
      }

      val metadataPath: Path = new Path(this.path, "_metadata")
      val metaDataFileStatus = fileStorage.hdfs.getFileStatus(metadataPath)
      val summaryList = parquetApiFactory.getFooterList(metaDataFileStatus)
      val filteredList = summaryList.filter(f => fileHasRows(f.getParquetMetadata))

      val sortedList = filteredList.sortWith((f1, f2) => {
        getFileNumber(f1.getFile) < getFileNumber(f2.getFile)
      })
      sortedList
    }
  }

  /**
   * retrieve a single value from the parquet file
   * @param creader ColumnReader for parquet file set to needed page
   * @param typeName The parquet type for the data in this column
   */
  private[parquet] def getValue(creader: ColumnReader, typeName: PrimitiveTypeName): Any = {
    typeName match {
      case PrimitiveTypeName.INT32 => creader.getInteger
      case PrimitiveTypeName.INT64 => creader.getLong
      case PrimitiveTypeName.BOOLEAN => creader.getBoolean
      case PrimitiveTypeName.FLOAT => creader.getFloat
      case PrimitiveTypeName.DOUBLE => creader.getDouble
      case _ =>
        val binary = creader.getBinary
        val data = binary.getBytes
        if (data == null)
          ""
        else {
          val buffer = decoder.decode(binary.toByteBuffer)
          buffer.toString
        }
    }
  }

}
