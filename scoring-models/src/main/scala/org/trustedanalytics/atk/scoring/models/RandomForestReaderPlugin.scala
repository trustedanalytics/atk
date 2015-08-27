/*
// Copyright (c) 2015 Intel Corporation 
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.trustedanalytics.atk.scoring.models

import java.io._

import org.trustedanalytics.atk.scoring.models.RandomForestJsonReadersWriters.RandomForestModelFormat
import org.apache.spark.mllib.tree.configuration.{ FeatureType, Algo }
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.trustedanalytics.atk.scoring.interfaces.{ Model, ModelLoader }
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model._
import org.apache.spark.mllib.linalg.{ DenseVector, SparseVector, Vector }
import spray.json._
import DefaultJsonProtocol._

object RandomForestJsonReadersWriters {

  implicit object AlgoFormat extends JsonFormat[Algo] {
    override def write(obj: Algo): JsValue = {
      JsObject("algo" -> obj.toString.toJson)
    }

    override def read(json: JsValue): Algo = {
      val fields = json.asJsObject.fields
      val a = getOrInvalid(fields, "algo").convertTo[String]
      Algo.withName(a)
    }
  }

  implicit object FeatureTypeFormat extends JsonFormat[FeatureType] {
    override def write(obj: FeatureType): JsValue = {
      JsObject("featuretype" -> obj.toString.toJson)
    }

    override def read(json: JsValue): FeatureType = {
      val fields = json.asJsObject.fields
      val f = getOrInvalid(fields, "featuretype").convertTo[String]
      FeatureType.withName(f)
    }
  }

  implicit object SplitFormat extends JsonFormat[Split] {
    override def write(obj: Split): JsValue = {
      JsObject("feature" -> obj.feature.toJson,
        "threshold" -> obj.threshold.toJson,
        "featuretype" -> FeatureTypeFormat.write(obj.featureType),
        "categories" -> obj.categories.toJson)
    }

    override def read(json: JsValue): Split = {
      val fields = json.asJsObject.fields
      val feature = getOrInvalid(fields, "feature").convertTo[Int]
      val threshold = getOrInvalid(fields, "threshold").convertTo[Double]
      val featureType = FeatureTypeFormat.read(getOrInvalid(fields, "featuretype"))
      val categories = getOrInvalid(fields, "categories").convertTo[List[Double]]
      new Split(feature, threshold, featureType, categories)
    }
  }

  implicit object PredictFormat extends JsonFormat[Predict] {
    override def write(obj: Predict): JsValue = {
      JsObject("predict" -> obj.predict.toJson,
        "prob" -> obj.prob.toJson)
    }

    override def read(json: JsValue): Predict = {
      val fields = json.asJsObject.fields
      val predict = getOrInvalid(fields, "predict").convertTo[Double]
      val prob = getOrInvalid(fields, "prob").convertTo[Double]
      new Predict(predict, prob)
    }
  }

  implicit object InformationGainStatsFormat extends JsonFormat[InformationGainStats] {
    override def write(obj: InformationGainStats): JsValue = {
      JsObject("gain" -> obj.gain.toJson,
        "impurity" -> obj.impurity.toJson,
        "leftimpurity" -> obj.leftImpurity.toJson,
        "rightimpurity" -> obj.rightImpurity.toJson,
        "leftpredict" -> PredictFormat.write(obj.leftPredict),
        "rightpredict" -> PredictFormat.write(obj.rightPredict)
      )
    }

    override def read(json: JsValue): InformationGainStats = {
      val fields = json.asJsObject.fields
      val gain = getOrInvalid(fields, "gain").convertTo[Double]
      val impurity = getOrInvalid(fields, "impurity").convertTo[Double]
      val leftImpurity = getOrInvalid(fields, "leftimpurity").convertTo[Double]
      val rightImpurity = getOrInvalid(fields, "rightimpurity").convertTo[Double]
      val leftPredict = PredictFormat.read(getOrInvalid(fields, "leftpredict"))
      val rightPredict = PredictFormat.read(getOrInvalid(fields, "rightpredict"))
      new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, leftPredict, rightPredict)
    }
  }

  implicit object NodeFormat extends JsonFormat[Node] {
    override def write(obj: Node): JsValue = {

      JsObject("id" -> obj.id.toJson,
        "predict" -> obj.predict.toJson,
        "impurity" -> obj.impurity.toJson,
        "isLeaf" -> obj.isLeaf.toJson,
        "split" -> obj.split.toJson,
        "leftNode" -> obj.leftNode.toJson,
        "rightNode" -> obj.rightNode.toJson,
        "stats" -> obj.stats.toJson)
    }

    override def read(json: JsValue): Node = {
      val fields = json.asJsObject.fields
      val id = getOrInvalid(fields, "id").convertTo[Int]
      val predict = getOrInvalid(fields, "predict").convertTo[Predict]
      val impurity = getOrInvalid(fields, "impurity").convertTo[Double]
      val isLeaf = getOrInvalid(fields, "isLeaf").convertTo[Boolean]
      val split = getOrInvalid(fields, "split").convertTo[Option[Split]]
      val leftNode = getOrInvalid(fields, "leftNode").convertTo[Option[Node]]
      val rightNode = getOrInvalid(fields, "rightNode").convertTo[Option[Node]]
      val stats = getOrInvalid(fields, "stats").convertTo[Option[InformationGainStats]]

      new Node(id, predict, impurity, isLeaf, split, leftNode, rightNode, stats)
    }
  }

  implicit object DecisionTreeModelFormat extends JsonFormat[DecisionTreeModel] {
    override def write(obj: DecisionTreeModel): JsValue = {
      JsObject("topnode" -> NodeFormat.write(obj.topNode),
        "algo" -> AlgoFormat.write(obj.algo))
    }

    override def read(json: JsValue): DecisionTreeModel = {
      val fields = json.asJsObject.fields
      val topNode = NodeFormat.read(getOrInvalid(fields, "topnode"))
      val algo = AlgoFormat.read(getOrInvalid(fields, "algo"))
      new DecisionTreeModel(topNode, algo)
    }
  }

  implicit object RandomForestModelFormat extends JsonFormat[RandomForestModel] {

    override def write(obj: RandomForestModel): JsValue = {
      JsObject("algo" -> AlgoFormat.write(obj.algo),
        "trees" -> new JsArray(obj.trees.map(t => DecisionTreeModelFormat.write(t)).toList))
    }

    override def read(json: JsValue): RandomForestModel = {
      val fields = json.asJsObject.fields
      val algo = AlgoFormat.read(getOrInvalid(fields, "algo"))
      val trees = getOrInvalid(fields, "trees").asInstanceOf[JsArray].elements.map(i => DecisionTreeModelFormat.read(i)).toArray
      new RandomForestModel(algo, trees)
    }
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }
}
class InvalidJsonException(message: String) extends RuntimeException(message)

class RandomForestReaderPlugin() extends ModelLoader {

  private var rfModel: RandomForestScoringModel = _

  override def load(bytes: Array[Byte]): Model = {
    try {
      val str = new String(bytes)
      println(str)
      val json: JsValue = str.parseJson
      val randomForestModel = json.convertTo[RandomForestModel]
      rfModel = new RandomForestScoringModel(randomForestModel)
      rfModel.asInstanceOf[Model]
    }
    catch {
      //TODO: log the error
      case e: IOException => throw e
    }
  }
}