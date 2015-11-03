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


package org.trustedanalytics.atk.engine.model.plugins.libsvm

import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import libsvm.svm_model
import spray.json._

/**
 * Implicit conversions for LibSvm objects to/from JSON
 */

object LibSvmJsonProtocol {

  implicit object SvmParameterFormat extends JsonFormat[libsvm.svm_parameter] {
    override def write(obj: libsvm.svm_parameter): JsValue = {
      JsObject(
        "svm_type" -> JsNumber(obj.svm_type),
        "kernel_type" -> JsNumber(obj.kernel_type),
        "degree" -> JsNumber(obj.degree),
        "gamma" -> JsNumber(obj.gamma),
        "coef0" -> JsNumber(obj.coef0),
        "cache_size" -> JsNumber(obj.cache_size),
        "eps" -> JsNumber(obj.eps),
        "C" -> JsNumber(obj.C),
        "nr_weight" -> JsNumber(obj.nr_weight),
        "weight_label" -> new JsArray(obj.weight_label.map(i => JsNumber(i)).toList),
        "weight" -> new JsArray(obj.weight.map(i => JsNumber(i)).toList),
        "nu" -> JsNumber(obj.nu),
        "p" -> JsNumber(obj.p),
        "shrinking" -> JsNumber(obj.shrinking),
        "probability" -> JsNumber(obj.probability)
      )
    }

    override def read(json: JsValue): libsvm.svm_parameter = {
      val fields = json.asJsObject.fields
      val svm_type = fields.get("svm_type").get.asInstanceOf[JsNumber].value.intValue()
      val kernel_type = fields.get("kernel_type").get.asInstanceOf[JsNumber].value.intValue()
      val degree = fields.get("degree").get.asInstanceOf[JsNumber].value.intValue()
      val gamma = fields.get("gamma").get.asInstanceOf[JsNumber].value.doubleValue()
      val coef0 = fields.get("coef0").get.asInstanceOf[JsNumber].value.doubleValue()
      val cache_size = fields.get("cache_size").get.asInstanceOf[JsNumber].value.doubleValue()
      val eps = fields.get("eps").get.asInstanceOf[JsNumber].value.doubleValue()
      val C = fields.get("C").get.asInstanceOf[JsNumber].value.doubleValue()
      val nr_weight = fields.get("nr_weight").get.asInstanceOf[JsNumber].value.intValue()
      val nu = fields.get("nu").get.asInstanceOf[JsNumber].value.doubleValue()
      val p = fields.get("p").get.asInstanceOf[JsNumber].value.doubleValue()
      val shrinking = fields.get("shrinking").get.asInstanceOf[JsNumber].value.intValue()
      val probability = fields.get("probability").get.asInstanceOf[JsNumber].value.intValue()
      val weight_label = fields.get("weight_label").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val weight = fields.get("weight").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray

      val svmParam = new libsvm.svm_parameter()
      svmParam.svm_type = svm_type
      svmParam.kernel_type = kernel_type
      svmParam.degree = degree
      svmParam.gamma = gamma
      svmParam.coef0 = coef0
      svmParam.cache_size = cache_size
      svmParam.eps = eps
      svmParam.C = C
      svmParam.nr_weight = nr_weight
      svmParam.nu = nu
      svmParam.p = p
      svmParam.shrinking = shrinking
      svmParam.probability = probability
      svmParam.weight = weight
      svmParam.weight_label = weight_label

      svmParam
    }
  }

  implicit object svm_node extends JsonFormat[libsvm.svm_node] {
    override def write(obj: libsvm.svm_node): JsValue = {
      JsObject(
        "index" -> JsNumber(obj.index),
        "value" -> JsNumber(obj.value)
      )
    }

    override def read(json: JsValue): libsvm.svm_node = {
      val fields = json.asJsObject.fields
      val index = fields.get("index").get.asInstanceOf[JsNumber].value.intValue()
      val value = fields.get("value").get.asInstanceOf[JsNumber].value.doubleValue()

      val svmNode = new libsvm.svm_node()
      svmNode.index = index
      svmNode.value = value

      svmNode
    }
  }

  implicit object LibSVMModelFormat extends JsonFormat[svm_model] {
    /**
     * The write methods converts from LibSVMModel to JsValue
     * @param obj svm_model
     * @return JsValue
     */
    override def write(obj: svm_model): JsValue = {
      //val t = if (obj.label == null) JsNull else new JsArray(obj.label.map(i => JsNumber(i)).toList)
      val checkLabel = obj.label match {
        case null => JsNull
        case _ => new JsArray(obj.label.map(i => JsNumber(i)).toList)
      }
      val checkProbA = obj.probA match {
        case null => JsNull
        case _ => new JsArray(obj.probA.map(d => JsNumber(d)).toList)
      }
      val checkProbB = obj.probB match {
        case null => JsNull
        case _ => new JsArray(obj.probB.map(d => JsNumber(d)).toList)
      }
      val checkNsv = obj.nSV match {
        case null => JsNull
        case _ => new JsArray(obj.nSV.map(d => JsNumber(d)).toList)
      }

      JsObject(
        "nr_class" -> JsNumber(obj.nr_class),
        "l" -> JsNumber(obj.l),
        "rho" -> new JsArray(obj.rho.map(i => JsNumber(i)).toList),
        "probA" -> checkProbA,
        "probB" -> checkProbB,
        "label" -> checkLabel,
        "sv_indices" -> new JsArray(obj.sv_indices.map(d => JsNumber(d)).toList),
        "sv_coef" -> new JsArray(obj.sv_coef.map(row => new JsArray(row.map(d => JsNumber(d)).toList)).toList),
        "nSV" -> checkNsv,
        "param" -> SvmParameterFormat.write(obj.param),
        "SV" -> new JsArray(obj.SV.map(row => new JsArray(row.map(d => svm_node.write(d)).toList)).toList)
      )
    }

    /**
     * The read method reads a JsValue to LibSVMModel
     * @param json JsValue
     * @return LibSvmModel
     */
    override def read(json: JsValue): svm_model = {
      val fields = json.asJsObject.fields
      val l = fields.get("l").get.asInstanceOf[JsNumber].value.intValue()
      val nr_class = fields.get("nr_class").get.asInstanceOf[JsNumber].value.intValue()
      val rho = fields.get("rho").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      val probA = fields.get("probA").get match {
        case JsNull => null
        case _ => fields.get("probA").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      }
      val probB = fields.get("probB").get match {
        case JsNull => null
        case _ => fields.get("probB").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      }
      val sv_indices = fields.get("sv_indices").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val sv_coef = fields.get("sv_coef").get.asInstanceOf[JsArray].elements.map(row => row.asInstanceOf[JsArray].elements.map(j => j.asInstanceOf[JsNumber].value.doubleValue()).toArray).toArray
      val label = fields.get("label").get match {
        case JsNull => null
        case _ => fields.get("label").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      }
      val nSV = fields.get("nSV").get match {
        case JsNull => null
        case _ => fields.get("nSV").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      }
      val param = fields.get("param").map(v => SvmParameterFormat.read(v)).get
      val SV = fields.get("SV").get.asInstanceOf[JsArray].elements.map(row => row.asInstanceOf[JsArray].elements.map(j => svm_node.read(j))toArray).toArray

      val svmModel = new svm_model()
      svmModel.l = l
      svmModel.nr_class = nr_class
      svmModel.rho = rho
      svmModel.probA = probA
      svmModel.probB = probB
      svmModel.sv_indices = sv_indices
      svmModel.sv_coef = sv_coef
      svmModel.label = label
      svmModel.nSV = nSV
      svmModel.param = param
      svmModel.SV = SV

      svmModel
    }
  }

  implicit val libSvmDataFormat = jsonFormat2(LibSvmData)
  implicit val libSvmModelFormat = jsonFormat19(LibSvmTrainArgs)
  implicit val libSvmPredictFormat = jsonFormat3(LibSvmPredictArgs)
  implicit val libSvmScoreFormat = jsonFormat2(LibSvmScoreArgs)
  implicit val libSvmScoreReturnFormat = jsonFormat1(LibSvmScoreReturn)
  implicit val libSvmTestFormat = jsonFormat4(LibSvmTestArgs)

}
