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

import java.io.{ InputStreamReader, BufferedReader, ByteArrayInputStream }

import libsvm.svm
import org.apache.spark.mllib.ScoringModelTestUtils
import org.scalatest.WordSpec

class LibSvmModelTest extends WordSpec {
  val data = new String("nr_class 2\ntotal_sv 2\nrho 0.5\nlabel 1 -1\nnr_sv 1 1\nSV\n1 1:0 2:1 3:1\n-1 4:2 1:0 2:2")
  val inputStream = new ByteArrayInputStream(data.getBytes())
  val reader = new BufferedReader(new InputStreamReader(inputStream))
  val obsCols = List("a", "b", "c")
  val libModel = svm.svm_load_model(reader)
  val libSvmModel = new LibSvmModel(libModel, new LibSvmData(libModel, obsCols))

  val numObsCols = obsCols.length

  "LibSvmModel" should {
    "throw an exception when attempting to score null data" in {
      ScoringModelTestUtils.nullDataTest(libSvmModel)
    }

    "throw an exception when scoring data with non-numerical records" in {
      ScoringModelTestUtils.invalidDataTest(libSvmModel, 3)
    }

    "successfully score a model when float data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 3)
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 1)
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 20)
    }

    "successfully score a model when integer data is provided" in {
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 3)
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 1)
      ScoringModelTestUtils.successfulModelScoringFloatTest(libSvmModel, 20)
    }

    "successfully return the observation columns used for training the model" in {
      ScoringModelTestUtils.successfulInputTest(libSvmModel, numObsCols)
    }

    "successfully return the observation columns used for training the model along with score" in {
      ScoringModelTestUtils.successfulOutputTest(libSvmModel, numObsCols)
    }
  }
}

