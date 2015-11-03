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

package org.trustedanalytics.atk.engine.frame.plugins.load

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.frame.load.HBaseSchemaArgs
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.apache.hadoop.hbase.KeyValue
import org.trustedanalytics.atk.engine.frame.plugins.load.HBasePlugin.LoadHBaseImpl
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

/**
 * Exercises the hbase create RDd functions.
 */
class HBaseTest extends TestingSparkContextFlatSpec with Matchers {

  trait LoadHBaseImplTest extends Serializable {

    val kvOne: KeyValue = new KeyValue(Bytes.toBytes(1) ++ Bytes.toBytes(5))
    val rowOneValue = Array[KeyValue]() :+ kvOne

    val kvTwo: KeyValue = new KeyValue(Bytes.toBytes(1) ++ Bytes.toBytes(10))
    val rowTwoValue = Array[KeyValue]() :+ kvTwo

    val row1 = (null, new Result(rowOneValue))
    val row2 = (null, new Result(rowTwoValue))

    val rowRDD = sparkContext.parallelize(List[(ImmutableBytesWritable, Result)](row1, row2))

    val schema: List[HBaseSchemaArgs] = List(
      HBaseSchemaArgs("pants", "isle", DataTypes.int32),
      HBaseSchemaArgs("pants", "row", DataTypes.int32)
    )
  }

  "LoadHBaseImpl::hbaseRddToRdd" should "complete on a 2 row table" in new LoadHBaseImplTest() {

    assert(rowRDD.count() == 2)
    val rdd = LoadHBaseImpl.hbaseRddToRdd(rowRDD, schema)
    assert(rdd != null)
  }
}
