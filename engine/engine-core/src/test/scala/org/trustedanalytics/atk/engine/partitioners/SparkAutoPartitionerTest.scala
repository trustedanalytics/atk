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

package org.trustedanalytics.atk.engine.partitioners

import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes, Schema }
import org.trustedanalytics.atk.engine.FileStorage
import org.apache.spark.frame.FrameRdd
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class SparkAutoPartitionerTest extends TestingSparkContextFlatSpec with MockitoSugar {

  val partitioner = new SparkAutoPartitioner(null)
  val rows = (1 to 100).map(i => Array(i.toLong, i.toString))
  val schema = FrameSchema(List(Column("num", DataTypes.int64), Column("name", DataTypes.string)))

  "SparkAutoPartitioner" should "calculate expected partitioning for VERY small files" in {
    assert(partitioner.partitionsFromFileSize(1) == 30)
  }

  it should "calculate the expected partitioning for small files" in {
    val tenMb = 10000000
    assert(partitioner.partitionsFromFileSize(tenMb) == 90)
  }

  it should "calculate max-partitions for VERY LARGE files" in {
    assert(partitioner.partitionsFromFileSize(Long.MaxValue) == 10000)
  }

  "repartitionFromFileSize" should "decrease the number of partitions" in {
    val fiveHundredKb = 50000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[FileStorage]
    when(mockHdfs.size("testpath")).thenReturn(fiveHundredKb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 180))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, shuffle = false)
    assert(repartitionedRdd.partitions.length == 30)

    val repartitionedRdd2 = repartitioner.repartition("testpath", frameRdd, shuffle = true)
    assert(repartitionedRdd2.partitions.length == 30)
  }

  "repartitionFromFileSize" should "increase the number of partitions when shuffle is true" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[FileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 30))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, shuffle = true)
    assert(repartitionedRdd.partitions.length == 90)
  }

  "repartitionFromFileSize" should "not increase the number of partitions when shuffle is false" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[FileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 30))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, shuffle = false)
    assert(repartitionedRdd.partitions.length == 30)
  }

  "repartitionFromFileSize" should "not re-partition if the percentage change is less than threshold" in {
    val tenMb = 10000000 // Re-partitioner multiplies this size by frame-compression-ratio
    val mockHdfs = mock[FileStorage]
    when(mockHdfs.size("testpath")).thenReturn(tenMb)

    val repartitioner = new SparkAutoPartitioner(mockHdfs)
    val frameRdd = FrameRdd.toFrameRdd(schema, sparkContext.parallelize(rows, 75))

    val repartitionedRdd = repartitioner.repartition("testpath", frameRdd, shuffle = true)
    assert(repartitionedRdd.partitions.length == 75)
  }
}
