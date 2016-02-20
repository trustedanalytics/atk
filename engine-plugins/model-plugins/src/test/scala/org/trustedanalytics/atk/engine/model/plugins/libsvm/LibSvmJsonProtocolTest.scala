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
package org.trustedanalytics.atk.engine.model.plugins.libsvm

//import org.apache.commons.math3.geometry.VectorFormat

import libsvm.{ svm_model, svm_node, svm_parameter }
import org.scalatest.WordSpec
import LibSvmJsonProtocol._
import org.trustedanalytics.atk.scoring.models.LibSvmData
import spray.json._

import scala.Array._

class LibSvmJsonProtocolTest extends WordSpec {

  "LibSvmDataFormat" should {

    "be able to serialize" in {
      val s = new LibSvmData(new svm_model(), List("column1", "column2", "columns3", "column4"))

      var myMatrix = ofDim[Double](3, 3)
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          myMatrix(i)(j) = j
        }
      }
      val myNode = new svm_node()
      myNode.index = 1
      myNode.value = 3.0

      var myNodeMatrix = ofDim[svm_node](3, 3)
      for (i <- 0 to 2) {
        for (j <- 0 to 2) {
          myNodeMatrix(i)(j) = myNode
        }
      }
      s.svmModel.l = 1
      s.svmModel.label = null
      s.svmModel.nr_class = 2
      s.svmModel.nSV = Array(1, 3, 5)
      s.svmModel.param = BuildParam()
      s.svmModel.SV = myNodeMatrix
      s.svmModel.probA = null
      s.svmModel.probB = null
      s.svmModel.rho = Array(1.0, 2.0)
      s.svmModel.sv_coef = myMatrix
      s.svmModel.sv_indices = Array(1, 3)
      assert(s.toJson.compactPrint == "{\"svm_model\":{\"nr_class\":2,\"l\":1,\"rho\":[1.0,2.0],\"probA\":null,\"probB\":null,\"label\":null,\"sv_indices\":[1,3],\"sv_coef\":[[0.0,1.0,2.0],[0.0,1.0,2.0],[0.0,1.0,2.0]],\"nSV\":[1,3,5],\"param\":{\"svm_type\":1,\"kernel_type\":2,\"degree\":4,\"gamma\":3.0,\"coef0\":4.0,\"cache_size\":3.0,\"eps\":3.0,\"C\":2.0,\"nr_weight\":1,\"weight_label\":[1,2],\"weight\":[1.0,2.0],\"nu\":1.0,\"p\":2.0,\"shrinking\":1,\"probability\":2},\"SV\":[[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}]]},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}")
    }

    "parse json" in {
      val string = "{\"svm_model\":{\"nr_class\":2,\"l\":1,\"rho\":[1.0,2.0],\"probA\":null,\"probB\":null,\"label\":null,\"sv_indices\":[1,3],\"sv_coef\":[[0.0,1.0,2.0],[0.0,1.0,2.0],[0.0,1.0,2.0]],\"nSV\":[1,3,5],\"param\":{\"svm_type\":1,\"kernel_type\":2,\"degree\":4,\"gamma\":3.0,\"coef0\":4.0,\"cache_size\":3.0,\"eps\":3.0,\"C\":2.0,\"nr_weight\":1,\"weight_label\":[1,2],\"weight\":[1.0,2.0],\"nu\":1.0,\"p\":2.0,\"shrinking\":1,\"probability\":2},\"SV\":[[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}],[{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0},{\"index\":1,\"value\":3.0}]]},\"observation_columns\":[\"column1\",\"column2\",\"columns3\",\"column4\"]}"
      val json = JsonParser(string).asJsObject
      val s = json.convertTo[LibSvmData]

      assert(s.svmModel.probA == null)
      assert(s.svmModel.l == 1)
      assert(s.observationColumns.length == 4)
      assert(s.svmModel.label == null)
      assert(s.svmModel.nr_class == 2)
      assert(s.svmModel.nSV.size == 3)
      assert(s.svmModel.probB == null)
      assert(s.svmModel.rho.size == 2)
      assert(s.svmModel.sv_indices.size == 2)
      assert(s.svmModel.param.svm_type == 1)
      assert(s.svmModel.param.kernel_type == 2)
      assert(s.svmModel.param.degree == 4)
      assert(s.svmModel.param.gamma == 3.0)
      assert(s.svmModel.param.coef0 == 4.0)
      assert(s.svmModel.param.cache_size == 3.0)
      assert(s.svmModel.param.eps == 3.0)
      assert(s.svmModel.param.C == 2.0)
      assert(s.svmModel.param.nr_weight == 1)
      assert(s.svmModel.param.weight_label.size == 2)
      assert(s.svmModel.param.weight.size == 2)
      assert(s.svmModel.param.nu == 1.0)
      assert(s.svmModel.param.p == 2.0)
      assert(s.svmModel.param.shrinking == 1)
      assert(s.svmModel.param.probability == 2)

    }

  }

  private def BuildParam(): svm_parameter = {

    val param = new svm_parameter()
    param.svm_type = 1
    param.kernel_type = 2
    param.degree = 4
    param.gamma = 3.0
    param.coef0 = 4.0
    param.cache_size = 3.0
    param.eps = 3.0
    param.C = 2.0
    param.nr_weight = 1
    param.weight_label = Array(1, 2)
    param.weight = Array(1.0, 2.0)
    param.nu = 1.0
    param.p = 2.0
    param.shrinking = 1
    param.probability = 2
    param
  }

}
