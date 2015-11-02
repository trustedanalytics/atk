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


package org.trustedanalytics.atk.engine.frame.parquet

import java.io.File

import org.trustedanalytics.atk.engine.FileStorage
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.{ BeforeAndAfterAll, Matchers, WordSpec }
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.testutils.DirectoryUtils
import parquet.column.page.PageReadStore
import parquet.column.{ ColumnDescriptor, ColumnReadStore, ColumnReader }
import parquet.hadoop.{ Footer, ParquetFileReader }
import parquet.hadoop.metadata.{ FileMetaData, ParquetMetadata, BlockMetaData }
import parquet.schema.{ MessageType, PrimitiveType, OriginalType }
import parquet.schema.PrimitiveType.PrimitiveTypeName
import scala.collection.JavaConverters._
import DirectoryUtils._

import org.mockito.Matchers._

case class TestClass(a: Int, b: Int)

class ParquetReaderTests extends WordSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  val maxRows = 20
  val typeList: java.util.List[parquet.schema.Type] = List(
    new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "a").asInstanceOf[parquet.schema.Type],
    new PrimitiveType(parquet.schema.Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "b").asInstanceOf[parquet.schema.Type]
  ).asJava
  val schema = new MessageType("root", typeList)

  /**
   * Create the mock columns that a parquetreadstore will return
   * @param valMod a modifier that is added to every number in the column so that each file can have unique values
   */
  def createMockReadStore(valMod: Int): ColumnReadStore = {
    val col1 = mock[ColumnReader]
    when(col1.getInteger).thenReturn(0 + valMod, 1 + valMod, 2 + valMod, 3 + valMod, 4 + valMod, 5 + valMod, 6 + valMod, 7 + valMod, 8 + valMod, 9 + valMod)
    val col2 = mock[ColumnReader]
    when(col2.getInteger).thenReturn(9 + valMod, 8 + valMod, 7 + valMod, 6 + valMod, 5 + valMod, 4 + valMod, 3 + valMod, 2 + valMod, 1 + valMod, 0 + valMod)
    val store = mock[ColumnReadStore]
    // The following two when calls will allow us to properly mock the ColumnReader.skip method which skips a row without reading it.
    when(col1.getCurrentDefinitionLevel).thenReturn(1)
    when(col1.skip()).thenAnswer(new Answer[Int] {
      override def answer(invocation: InvocationOnMock): Int = {
        invocation.getMock.asInstanceOf[ColumnReader].getInteger
      }
    })
    when(col2.getCurrentDefinitionLevel).thenReturn(1)
    when(col2.skip()).thenAnswer(new Answer[Int] {
      override def answer(invocation: InvocationOnMock): Int = {
        invocation.getMock.asInstanceOf[ColumnReader].getInteger
      }
    })
    when(store.getColumnReader(schema.getColumns.get(0))).thenReturn(col1)
    when(store.getColumnReader(schema.getColumns.get(1))).thenReturn(col2)
    store
  }

  /**
   * Creates a mock ParquetFileReaderFactory ready and able to return two mock
   * pageReadStores. Setting the fileReaderfactory for a parquetFileReader should handle
   * unit test mocks. The page read stores return the column read stores with the same info
   */
  def createMockParquetFileReaderFactory(): ParquetApiFactory = {
    val factory = mock[ParquetApiFactory]
    val reader = mock[ParquetFileReader]
    val store = mock[PageReadStore]
    when(store.getRowCount()).thenReturn(10)
    when(reader.readNextRowGroup()).thenReturn(store, null, store, null)

    when(factory.newParquetFileReader(anyObject(), anyObject(), anyObject())).thenReturn(reader, reader, null)
    val mockReadStore1 = createMockReadStore(0)
    val mockReadStore2 = createMockReadStore(10)
    when(factory.newColumnReadStore(anyObject(), anyObject())).thenReturn(mockReadStore1, mockReadStore2, null)

    val mockFooters = List(createMockFooter("part-r-2.parquet", 10),
      createMockFooter("part-r-3.parquet", 0), createMockFooter("part-r-1.parquet", 10))
    when(factory.getFooterList(anyObject())).thenReturn(mockFooters)
    factory
  }

  /**
   * Create a mock footer so that the parquet reader can test the getFiles method
   * @param name Mock File name
   * @param rowCount Mock Row Count
   */
  def createMockFooter(name: String, rowCount: Long): Footer = {
    val file = mock[Path]
    when(file.getName).thenReturn(name)
    val metaData = mock[ParquetMetadata]
    val block = mock[BlockMetaData]
    when(block.getRowCount()).thenReturn(rowCount)
    when(metaData.getBlocks()).thenReturn(List(block).asJava)
    val fileMetaData = new FileMetaData(schema, mock[java.util.Map[String, String]], "")
    when(metaData.getFileMetaData()).thenReturn(fileMetaData)
    new Footer(file, metaData)
  }

  /**
   * Create a real ParquetReader with all mocks in place for the majority of tests.
   */
  def createTestParquetReader(): ParquetReader = {
    val mockFactory = createMockParquetFileReaderFactory()
    val fs = mock[FileStorage]
    val conf: Configuration = new Configuration()
    val fileSystem = mock[FileSystem]
    when(fileSystem.isDirectory(anyObject())).thenReturn(true)
    when(fs.configuration).thenReturn(conf)
    when(fs.hdfs).thenReturn(fileSystem)

    new ParquetReader(new Path("TestPath"), fs, mockFactory)
  }

  "ParquetReader.getValue" should {
    "return the expected type" in {

      val rdr = createTestParquetReader()
      val creader = mock[ColumnReader]
      when(creader.getInteger).thenReturn(1, 10)
      when(creader.getLong).thenReturn(2l)
      when(creader.getBoolean).thenReturn(false)
      when(creader.getFloat).thenReturn(3.0f)
      when(creader.getDouble).thenReturn(4.0)
      val encoder = rdr.utf8.newEncoder()
      val encoded = encoder.encode(java.nio.CharBuffer.wrap("Hello World"))
      val binary = parquet.io.api.Binary.fromByteBuffer(encoded)
      when(creader.getBinary()).thenReturn(binary)

      rdr.getValue(creader, PrimitiveTypeName.INT32) should be(1)
      rdr.getValue(creader, PrimitiveTypeName.INT32) should be(10)
      rdr.getValue(creader, PrimitiveTypeName.INT64) should be(2L)
      rdr.getValue(creader, PrimitiveTypeName.FLOAT) should be(3.0f)
      rdr.getValue(creader, PrimitiveTypeName.DOUBLE) should be(4.0)
      rdr.getValue(creader, PrimitiveTypeName.BOOLEAN).toString should be(false.toString)
      rdr.getValue(creader, PrimitiveTypeName.BINARY) should be("Hello World")
    }
  }

  "ParquetFileReader.getFiles" should {
    val rdr = createTestParquetReader()
    val files = rdr.getFiles()
    "return more than one file if it is a directory" in {
      files.length should be > 1
    }
    "files with no rows should not be included in the file list" in {
      files.foreach(f => {
        val sum = f.getParquetMetadata.getBlocks.asScala.map(b => b.getRowCount).sum
        sum should not be 0
      })
    }
    "files should be sorted by in order of part number" in {
      files(0).getFile.getName should include("1")
      files(1).getFile.getName should include("2")
    }
  }

  "ParquetReader" should {
    "return the requested number of rows" in {
      val rdr = createTestParquetReader()
      val results = rdr.take(10, 0, Some(100))
      results.length should equal(10)
    }

    "limit the returned rows based on configured restrictions" in {
      val rdr = createTestParquetReader()
      rdr.take(10, 0, Some(5)).length should equal(5)
    }
    "return no more rows than are available" in {
      val rdr = createTestParquetReader()
      rdr.take(maxRows * 2, 0, None).length should equal(maxRows)
    }
    "return no more rows than are available with an offset" in {
      val rdr = createTestParquetReader()
      rdr.take(maxRows * 2, 5, None).length should equal(maxRows - 5)
    }
    "start at the requested offset" in {
      val rdr = createTestParquetReader()
      rdr.take(maxRows, maxRows - 10, None).length should equal(10)
    }
    "return no rows when a zero count is requested" in {
      val rdr = createTestParquetReader()
      rdr.take(0, 0, Some(100)).length should equal(0)
    }
    "not generate the same row twice" in {
      val rdr = createTestParquetReader()
      val results = rdr.take(maxRows, 0, None)
      results.groupBy { case Array(index, _) => index }.count(_._2.length > 1) should equal(0)
    }

    "the expected data for the first 5 rows should be correct" in {
      val rdr = createTestParquetReader()
      val results = rdr.take(5, 0, None)
      (0 to 4).map(i => {
        results(i)(0) should be(i)
        results(i)(1) should be(9 - i)
      })
    }

    "the expected data for the rows 8-14 should be correct" in {
      val rdr = createTestParquetReader()
      val results = rdr.take(5, 8, None)
      results(0) should equal(Array(8, 1))
      results(1) should equal(Array(9, 0))
      results(2) should equal(Array(10, 19))
      results(3) should equal(Array(11, 18))
      results(4) should equal(Array(12, 17))
    }
  }

}
