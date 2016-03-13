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
package org.trustedanalytics.atk.engine.frame.plugins.statistics.covariance

import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, DataTypes }
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.apache.spark.sql.Row
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class CovarianceMatrixTest extends TestingSparkContextFlatSpec with Matchers {
  val inputArray: Array[Array[Double]] = Array(Array(90.0, 60.0, 90.0), Array(90.0, 90.0, 30.0),
    Array(60.0, 60.0, 60.0), Array(60.0, 60.0, 90.0), Array(30.0, 30.0, 30.0))

  "CovarianceFunctions matrix calculations" should "return the correct values" in {

    val arrGenericRow: Array[Row] = inputArray.map(row => {
      val temp: Array[Any] = row.map(x => x)
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val columnsList = List("col_0", "col_1", "col_2")
    val inputDataColumnNamesAndTypes: List[Column] = columnsList.map({ name => Column(name, DataTypes.float64) })
    val schema = FrameSchema(inputDataColumnNamesAndTypes)
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, columnsList).collect()
    result.size shouldBe 3
    result(0) shouldBe Row(630.0, 450.0, 225.0)
    result(1) shouldBe Row(450.0, 450.0, 0.0)
    result(2) shouldBe Row(225.0, 0.0, 900.0)
  }
  "CovarianceFunctions matrix calculations" should "return the correct values for vector data types" in {
    val arrGenericRow: Array[Row] = inputArray.map(row => {
      val temp: Array[Any] = Array(DataTypes.toVector(3)(row))
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val schema = FrameSchema(List(Column("col_0", DataTypes.vector(3))))
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, List("col_0"), outputVectorLength = Some(3)).collect()

    result.size shouldBe 3
    result(0)(0) shouldBe Vector(630.0, 450.0, 225.0)
    result(1)(0) shouldBe Vector(450.0, 450.0, 0.0)
    result(2)(0) shouldBe Vector(225.0, 0.0, 900.0)
  }
  "CovarianceFunctions matrix calculations" should "return the correct values for mixed vector and numeric data types" in {
    val arrGenericRow: Array[Row] = inputArray.map(row => {
      val temp: Array[Any] = Array(DataTypes.toVector(2)(row.slice(0, 2)), row(2))
      new GenericRow(temp)
    })

    val rdd = sparkContext.parallelize(arrGenericRow)
    val schema = FrameSchema(List(Column("col_0", DataTypes.vector(2)), Column("col_1", DataTypes.float64)))
    val frameRdd = new FrameRdd(schema, rdd)
    val result = CovarianceFunctions.covarianceMatrix(frameRdd, List("col_0", "col_1")).collect()

    result.size shouldBe 3
    result(0) shouldBe Row(630.0, 450.0, 225.0)
    result(1) shouldBe Row(450.0, 450.0, 0.0)
    result(2) shouldBe Row(225.0, 0.0, 900.0)
  }

}
