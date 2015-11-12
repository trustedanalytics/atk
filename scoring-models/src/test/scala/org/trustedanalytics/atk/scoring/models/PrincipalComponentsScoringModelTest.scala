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

package org.trustedanalytics.atk.scoring.models

import org.apache.spark.mllib.ScoringJsonReaderWriters
import org.apache.spark.mllib.linalg.{ Matrices, DenseMatrix, Vectors }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ Matchers, WordSpec }
import spray.json._
import ScoringJsonReaderWriters._

class PrincipalComponentsScoringModelTest extends WordSpec with Matchers with ScalaFutures {

  "computePrincipalComponents" should {
    "compute the princ" in {
      val columnMeans = Vectors.dense(Array(3.0, 1.56999, 0.3))
      val singularValues = Vectors.dense(Array(1.95285, 1.25895, 0.34988))
      val vFactor = Matrices.dense(3, 3, Array(-0.98806, -0.14751, 0.04444, 0.152455, -0.9777, 0.14391, 0.02222, 0.14896, 0.98859))

      val pcaDataModel = PrincipalComponentsData(3, List("1", "2", "3"), true, columnMeans, singularValues, vFactor)
      val scoringModel = new PrincipalComponentsScoringModel(pcaDataModel)
      val principalComponents = scoringModel.computePrincipalComponents(Array(2.6, 1.7, 0.3))
      principalComponents.values(0) should equal(0.3760462248999999)
      principalComponents.values(1) should equal(-0.18809277699999993)
      principalComponents.values(2) should equal(0.010478289599999998)

    }
  }

  "computeTSquaredIndex" should {
    "compute the t squared index" in {
      val y: Array[Double] = Array(0.376046, -0.188093, 0.010475)
      val singularValues = Vectors.dense(Array(1.95285, 1.25895, 0.34988))
      val columnMeans = Vectors.dense(Array(3.0, 1.56999, 0.3))
      val vFactor = Matrices.dense(3, 3, Array(-0.98806, -0.14751, 0.04444, 0.152455, -0.9777, 0.14391, 0.02222, 0.14896, 0.98859))
      val pcaDataModelMeanCentered = PrincipalComponentsData(3, List("1", "2", "3"), true, columnMeans, singularValues, vFactor)
      val scoringModelMeanCentered = new PrincipalComponentsScoringModel(pcaDataModelMeanCentered)
      val tSquaredIndexMeanCentered = scoringModelMeanCentered.computeTSquaredIndex(y, singularValues, 3)
      tSquaredIndexMeanCentered should equal(0.06029846700647545)

      val pcaDataModel = PrincipalComponentsData(3, List("1", "2", "3"), false, columnMeans, singularValues, vFactor)
      val scoringModel = new PrincipalComponentsScoringModel(pcaDataModel)
      val y1: Array[Double] = Array(-2.806404, -1.222661, 0.607612)
      val tSquaredIndex = scoringModel.computeTSquaredIndex(y1, singularValues, 3)
      tSquaredIndex should equal(6.024266305651189)
    }

  }
}

