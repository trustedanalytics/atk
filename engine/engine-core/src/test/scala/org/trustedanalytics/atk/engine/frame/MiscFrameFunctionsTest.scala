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
package org.trustedanalytics.atk.engine.frame

import org.scalatest.Matchers
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

import scala.collection.mutable.ArrayBuffer

class MiscFrameFunctionsTest extends TestingSparkContextFlatSpec with Matchers {

  val max = 20
  val array = (1 to max * 2).map(i => Array(i, i.toString, i.toDouble * 0.1))

  def fetchAllData(): ArrayBuffer[Array[Any]] = {
    val data = sparkContext.parallelize(array)
    val results = new ArrayBuffer[Array[Any]]()
    var offset = 0
    var loop = true
    while (loop) {
      val batch = MiscFrameFunctions.getRows(data, offset, max, max)
      if (batch.length == 0)
        loop = false
      offset += max
      results ++= batch
    }
    results
  }

  "getRows" should "return the requested number of rows" in {
    val data = sparkContext.parallelize(array)
    MiscFrameFunctions.getRows(data, 0, max, max).length should equal(max)
  }

  it should "limit the returned rows based on configured restrictions" in {
    val data = sparkContext.parallelize(array)
    MiscFrameFunctions.getRows(data, 0, max + 5, max).length should equal(max)
  }

  it should "return no more rows than are available" in {
    val data = sparkContext.parallelize(array)
    MiscFrameFunctions.getRows(data, max * 2 - 5, max, max).length should equal(5)
  }

  it should "start at the requested offset" in {
    val data = sparkContext.parallelize(array)
    MiscFrameFunctions.getRows(data, max * 2 - 10, 5, max).length should equal(5)
  }

  it should "return no rows when a zero count is requested" in {
    val data = sparkContext.parallelize(array)
    MiscFrameFunctions.getRows(data, max * 2 - 10, 0, max).length should equal(0)
  }

  it should "return all the data when invoked enough times" in {
    val results = fetchAllData()

    results.length should equal(array.length)
  }

  it should "not generate the same row twice" in {
    val results = fetchAllData()

    results.groupBy { case Array(index, _, _) => index }.count(_._2.length > 1) should equal(0)
  }

  "getRows" should "be able to return non row objects" in {
    val data = sparkContext.parallelize(List.range(0, 100))

    val results = MiscFrameFunctions.getRows(data, 0, max, max)
    results(0).getClass should equal(Integer.TYPE)
  }
}
