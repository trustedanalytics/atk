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
package org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction

import org.apache.spark.frame.FrameRdd
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.domain.schema.{Column, DataTypes, FrameSchema}
import org.trustedanalytics.atk.testutils.MatcherUtils._
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class PrincipalComponentsFunctionsTest  extends TestingSparkContextWordSpec with Matchers with MockitoSugar{

  val schema = FrameSchema(List(
    Column("col_0", DataTypes.float64),
    Column("col_1", DataTypes.float64),
    Column("col_2", DataTypes.float64),
    Column("col_3", DataTypes.float64),
    Column("col_4", DataTypes.float64)
  ))

  val data : List[Row] = List(
    new GenericRow(Array[Any](0.0, 1.0, 0.0, 7.0, 0.0)),
    new GenericRow(Array[Any](2.0, 0.0, 3.0, 4.0, 5.0)),
    new GenericRow(Array[Any](4.0, 0.0, 0.0, 6.0, 7.0))
  )

  val columnMeans = Vectors.dense(Array(3.0, 1.56999, 0.3))
  val singularValues = Vectors.dense(Array(1.95285, 1.25895, 0.34988))
  val vFactor = Matrices.dense(3, 3, Array(-0.98806, -0.14751, 0.04444, 0.152455, -0.9777, 0.14391, 0.02222, 0.14896, 0.98859))

  val predictSchema = FrameSchema(List(
    Column("col_0", DataTypes.float64),
    Column("col_1", DataTypes.float64),
    Column("col_2", DataTypes.float64)
  ))

  val predictInput : List[Row] = List(
    new GenericRow(Array[Any](-0.4, 0.13001, 0.0)),
    new GenericRow(Array[Any](3.6, 1.25, 1.0))
  )
  
  "PrincipalComponentsFunctions" should {

    "predict principal components and t-square index without mean-centering" in {
      val rows = sparkContext.parallelize(predictInput)
      val frameRdd = new FrameRdd(predictSchema, rows)
      val columns = List("col_0", "col_1", "col_2")
      val modelData = PrincipalComponentsData(3, columns, false, columnMeans, singularValues, vFactor)

      val resultsFrame = PrincipalComponentsFunctions.predictPrincipalComponents(frameRdd, modelData,
        columns, 3, false, true)
      val resultArray = resultsFrame.map(row => {
        for (i <- 0 until row.length) yield row.getDouble(i)
      }).collect()

      resultArray.length should equal(2)
      resultArray(0).toArray should equalWithTolerance(Array(-0.4, 0.13001, 0.0, 0.376046, -0.188093, 0.0104783,0.060298))
      resultArray(1).toArray should equalWithTolerance(Array(3.6, 1.25, 1.0, -3.696964,-0.529377,1.254782, 16.622385))
    }

    "predict principal components with mean-centering" in {
      val rows = sparkContext.parallelize(predictInput)
      val frameRdd = new FrameRdd(predictSchema, rows)
      val columns = List("col_0", "col_1", "col_2")

      val modelData = PrincipalComponentsData(3, columns, false, columnMeans, singularValues, vFactor)

      val resultsFrame = PrincipalComponentsFunctions.predictPrincipalComponents(frameRdd, modelData,
        columns, 3, true, false)
      val resultArray = resultsFrame.map(row => {
        for (i <- 0 until row.length) yield row.getDouble(i)
      }).collect()

      resultArray.length should equal(2)
      resultArray(0).toArray should equalWithTolerance(Array(-0.4, 0.13001, 0.0, 2.036505, 0.170642, -0.622152 ))
      resultArray(1).toArray should equalWithTolerance(Array(3.6, 1.25, 1.0, -2.036505, -0.170642, 0.622152))
    }
    
    "compute the principal components" in {
      val rows = sparkContext.parallelize(predictInput)
      val frameRdd = new FrameRdd(predictSchema, rows)
      val columns = List("col_0", "col_1", "col_2")

      val matrix = PrincipalComponentsFunctions.toIndexedRowMatrix(frameRdd, columns, false)
      val modelData = PrincipalComponentsData(3, columns, true, columnMeans, singularValues, vFactor)
      val principalComponents = PrincipalComponentsFunctions.computePrincipalComponents(modelData, 3, matrix)

      principalComponents.numCols() should equal(3)
      principalComponents.numRows() should equal(2)

      val vectors = principalComponents.rows.collect()
      vectors(0).vector.toArray should equalWithTolerance(Array(0.376046, -0.188093, 0.0104782))
      vectors(1).vector.toArray should equalWithTolerance(Array(-3.696964,-0.529377,1.254782))
    }
    
    "compute the t-squared index" in {
      val y : List[Row] = List(
        new GenericRow(Array[Any](0.376046, -0.188093, 0.010475)),
        new GenericRow(Array[Any](-3.696964,-0.529377,1.254782)),
        new GenericRow(Array[Any](-2.806404, -1.222661, 0.607612))
      )

      val rows = sparkContext.parallelize(y)
      val frameRdd = new FrameRdd(predictSchema, rows)
      val columns = List("col_0", "col_1", "col_2")

      val matrix = PrincipalComponentsFunctions.toIndexedRowMatrix(frameRdd, columns, false)
      val tSquaredIndexMeanCentered = PrincipalComponentsFunctions.computeTSquaredIndex(matrix, singularValues, 3)

      tSquaredIndexMeanCentered.numCols() should equal(4)
      tSquaredIndexMeanCentered.numRows() should equal(3)

      val vectors = tSquaredIndexMeanCentered.rows.collect()
      vectors(0).vector.toArray should equalWithTolerance(Array(0.376046, -0.188093, 0.0104783,0.060298))
      vectors(1).vector.toArray should equalWithTolerance(Array(-3.696964,-0.529377,1.254782, 16.622385))
      vectors(2).vector.toArray should equalWithTolerance(Array(-2.806404, -1.222661, 0.607612,6.024266))
    }
    
    "convert frame to vector RDD" in {
      val rows = sparkContext.parallelize(data)
      val frameRdd = new FrameRdd(schema, rows)
      val vectors = PrincipalComponentsFunctions.toVectorRdd(frameRdd, List("col_0", "col_3"), false).collect()
      vectors.length should equal(3)
      vectors(0).toArray should equalWithTolerance(Array(0.0, 7.0))
      vectors(1).toArray should equalWithTolerance(Array(2.0, 4.0))
      vectors(2).toArray should equalWithTolerance(Array(4.0, 6.0))
    }

    "convert frame to mean-centered vector RDD" in {
      val rows = sparkContext.parallelize(data)
      val frameRdd = new FrameRdd(schema, rows)
      val vectors = PrincipalComponentsFunctions.toVectorRdd(frameRdd, List("col_0", "col_3"), true).collect()
      vectors.length should equal(3)
      vectors(0).toArray should equalWithTolerance(Array(-2.0, 1.3333333))
      vectors(1).toArray should equalWithTolerance(Array(0, -1.6666667))
      vectors(2).toArray should equalWithTolerance(Array(2.0, 0.3333333))
    }

    "convert frame to row matrix" in {
      val rows = sparkContext.parallelize(data)
      val frameRdd = new FrameRdd(schema, rows)
      val matrix = PrincipalComponentsFunctions.toRowMatrix(frameRdd, List("col_0", "col_3"), false)
      matrix.numCols() should equal(2)
      matrix.numRows() should equal(3)

      val vectors = matrix.rows.collect()
      vectors(0).toArray should equalWithTolerance(Array(0.0, 7.0))
      vectors(1).toArray should equalWithTolerance(Array(2.0, 4.0))
      vectors(2).toArray should equalWithTolerance(Array(4.0, 6.0))
    }

    "convert frame to indexed row matrix" in {
      val rows = sparkContext.parallelize(data)
      val frameRdd = new FrameRdd(schema, rows)
      val matrix = PrincipalComponentsFunctions.toRowMatrix(frameRdd, List("col_0", "col_3"), false)
      matrix.numCols() should equal(2)
      matrix.numRows() should equal(3)

      val vectors = matrix.rows.collect()
      vectors(0).toArray should equalWithTolerance(Array(0.0, 7.0))
      vectors(1).toArray should equalWithTolerance(Array(2.0, 4.0))
      vectors(2).toArray should equalWithTolerance(Array(4.0, 6.0))
    }

    "convert frame to mean-centered indexed row matrix" in {
      val rows = sparkContext.parallelize(data)
      val frameRdd = new FrameRdd(schema, rows)

      val matrix = PrincipalComponentsFunctions.toIndexedRowMatrix(frameRdd, List("col_0", "col_3"), true)
      matrix.numCols() should equal(2)
      matrix.numRows() should equal(3)

      val vectors = matrix.rows.collect()
      vectors(0).index should equal(0)
      vectors(1).index should equal(1)
      vectors(2).index should equal(2)
      vectors(0).vector.toArray should equalWithTolerance(Array(-2.0, 1.3333333))
      vectors(1).vector.toArray should equalWithTolerance(Array(0, -1.6666667))
      vectors(2).vector.toArray should equalWithTolerance(Array(2.0, 0.3333333))
    }
  }

}
