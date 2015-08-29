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

package org.apache.spark.mllib.atk.plugins

import org.apache.spark.mllib.classification.{ LogisticRegressionModelWithFrequency, NaiveBayesModel, SVMModel }
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{ DenseMatrix, DenseVector, Matrix, SparseVector, Vector }
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
import org.trustedanalytics.atk.engine.model.plugins.classification._
import org.trustedanalytics.atk.engine.model.plugins.classification.glm.{ LogisticRegressionData, LogisticRegressionSummaryTable, LogisticRegressionTrainArgs }
import org.trustedanalytics.atk.engine.model.plugins.clustering.{ KMeansData, KMeansPredictArgs, KMeansTrainArgs, KMeansTrainReturn }
import org.trustedanalytics.atk.engine.model.plugins.dimensionalityreduction._
import org.trustedanalytics.atk.engine.model.plugins.regression.LinearRegressionData
import org.trustedanalytics.atk.engine.model.plugins.scoring.ModelPublishArgs
import spray.json._

/**
 * Implicit conversions for Logistic Regression objects to/from JSON
 */

object MLLibJsonProtocol {

  implicit object SparseVectorFormat extends JsonFormat[SparseVector] {
    /**
     * Conversion from MLLib's SparseVector format to JsValue
     * @param obj: SparseVector whose format is SparseVector(val size: Int, val indices: Array[Int], val values: Array[Double])
     * @return JsValue
     */
    override def write(obj: SparseVector): JsValue = {
      JsObject(
        "size" -> JsNumber(obj.size),
        "indices" -> new JsArray(obj.indices.map(i => JsNumber(i)).toList),
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }

    /**
     * Conversion from JsValue to MLLib's SparseVector format
     * @param json: JsValue
     * @return SparseVector whose format is SparseVector(val size: Int, val indices: Array[Int], val values: Array[Double])
     */
    override def read(json: JsValue): SparseVector = {
      val fields = json.asJsObject.fields
      val size = fields.get("size").get.asInstanceOf[JsNumber].value.intValue
      val indices = fields.get("indices").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue).toArray
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray

      new SparseVector(size, indices, values)
    }
  }

  implicit object DenseVectorFormat extends JsonFormat[DenseVector] {
    /**
     * Conversion from MLLib's DenseVector format to JsValue
     * @param obj DenseVector, whose format is DenseVector(val values: Array[Double])
     * @return JsValue
     */
    override def write(obj: DenseVector): JsValue = {
      JsObject(
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList)
      )
    }

    /**
     * Conversion from JsValue to MLLib's DenseVector format
     * @param json JsValue
     * @return DenseVector, whose format is DenseVector(val values: Array[Double])
     */
    override def read(json: JsValue): DenseVector = {
      val fields = json.asJsObject.fields
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray
      new DenseVector(values)
    }
  }

  implicit object LogisticRegressionModelFormat extends JsonFormat[LogisticRegressionModelWithFrequency] {
    /**
     * The write methods converts from LogisticRegressionModel to JsValue
     * @param obj LogisticRegressionModel. Where LogisticRegressionModel's format is
     *            LogisticRegressionModel(val weights: Vector,val intercept: Double, numFeatures:Int, numClasses: Int)
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: LogisticRegressionModelWithFrequency): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept),
        "numFeatures" -> JsNumber(obj.numFeatures),
        "numClasses" -> JsNumber(obj.numClasses)
      )
    }

    /**
     * The read method reads a JsValue to LogisticRegressionModel
     * @param json JsValue
     * @return LogisticRegressionModel with format LogisticRegressionModel(val weights: Vector,val intercept: Double, numfeatures:Int, numClasses:Int)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): LogisticRegressionModelWithFrequency = {
      val fields = json.asJsObject.fields

      val intercept = fields.getOrElse("intercept", throw new IllegalArgumentException("Error in de-serialization: Missing intercept."))
        .asInstanceOf[JsNumber].value.doubleValue()
      val numFeatures = fields.getOrElse("numFeatures", throw new IllegalArgumentException("Error in de-serialization: Missing numFeatures"))
        .asInstanceOf[JsNumber].value.intValue()
      val numClasses = fields.getOrElse("numClasses", throw new IllegalArgumentException("Error in de-serialization: Missing numClasses"))
        .asInstanceOf[JsNumber].value.intValue()

      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }
      ).get

      new LogisticRegressionModelWithFrequency(weights, intercept, numFeatures, numClasses)
    }

  }

  implicit object LinearRegressionModelFormat extends JsonFormat[LinearRegressionModel] {
    /**
     * The write methods converts from LinearRegressionModel to JsValue
     * @param obj LinearRegressionModel. Where LinearRegressionModel's format is
     *            LinearRegressionModel(val weights: Vector,val intercept: Double)
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: LinearRegressionModel): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    /**
     * The read method reads a JsValue to LinearRegressionModel
     * @param json JsValue
     * @return LinearRegressionModel with format LinearRegressionModel(val weights: Vector,val intercept: Double)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): LinearRegressionModel = {
      val fields = json.asJsObject.fields
      val intercept = fields.getOrElse("intercept", throw new IllegalArgumentException("Error in de-serialization: Missing intercept."))
        .asInstanceOf[JsNumber].value.doubleValue()

      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }
      ).get

      new LinearRegressionModel(weights, intercept)
    }

  }

  implicit object VectorFormat extends JsonFormat[Vector] {
    override def write(obj: Vector): JsValue = {
      obj match {
        case sv: SparseVector => SparseVectorFormat.write(sv)
        case dv: DenseVector => DenseVectorFormat.write(dv)
        case _ => throw new IllegalArgumentException("Object does not confirm to Vector format.")
      }
    }

    override def read(json: JsValue): Vector = {
      if (json.asJsObject.fields.get("size").isDefined) {
        SparseVectorFormat.read(json)
      }
      else {
        DenseVectorFormat.read(json)
      }
    }
  }

  implicit object DenseMatrixFormat extends JsonFormat[DenseMatrix] {
    override def write(obj: DenseMatrix): JsValue = {
      JsObject(
        "numRows" -> JsNumber(obj.numRows),
        "numCols" -> JsNumber(obj.numCols),
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList),
        "isTransposed" -> JsBoolean(obj.isTransposed)
      )
    }

    override def read(json: JsValue): DenseMatrix = {
      val fields = json.asJsObject.fields

      val numRows = getOrInvalid(fields, "numRows").convertTo[Int]
      val numCols = getOrInvalid(fields, "numCols").convertTo[Int]
      val values = fields.get("values").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray
      val isTransposed = getOrInvalid(fields, "isTransposed").convertTo[Boolean]

      new DenseMatrix(numRows, numCols, values, isTransposed)
    }
  }

  implicit object MatrixFormat extends JsonFormat[Matrix] {
    override def write(obj: Matrix): JsValue = {
      obj match {

        case dm: DenseMatrix => DenseMatrixFormat.write(dm)
        case _ => throw new IllegalArgumentException("Objects doe not confirm to DenseMatrix format")
      }
    }

    override def read(json: JsValue): Matrix = {
      DenseMatrixFormat.read(json)
    }
  }

  implicit object KmeansModelFormat extends JsonFormat[KMeansModel] {
    /**
     * The write methods converts from KMeans to JsValue
     * @param obj KMeansModel. Where KMeansModel's format is
     *            val clusterCenters: Array[Vector]
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: KMeansModel): JsValue = {
      val centers = obj.clusterCenters.map(vector => VectorFormat.write(vector))
      JsObject("clusterCenters" -> JsArray(centers.toList))
    }

    /**
     * The read method reads a JsValue to KMeansModel
     * @param json JsValue
     * @return KMeansModel with format KMeansModel(val clusterCenters:Array[Vector])
     *         where Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): KMeansModel = {
      val fields = json.asJsObject.fields

      val centers = fields.get("clusterCenters").get.asInstanceOf[JsArray].elements.map(vector => {
        VectorFormat.read(vector)
      })

      new KMeansModel(centers.toArray)
    }

  }

  implicit object SVMModelFormat extends JsonFormat[SVMModel] {
    /**
     * The write methods converts from SVMModel to JsValue
     * @param obj SVMModel. Where SVMModel's format is
     *            SVMModel(val weights: Vector,val intercept: Double)
     *            and the weights Vector could be either a SparseVector or DenseVector
     * @return JsValue
     */
    override def write(obj: SVMModel): JsValue = {
      val weights = VectorFormat.write(obj.weights)
      JsObject(
        "weights" -> weights,
        "intercept" -> JsNumber(obj.intercept)
      )
    }

    /**
     * The read method reads a JsValue to SVMModel
     * @param json JsValue
     * @return SVMModel with format SVMModel(val weights: Vector,val intercept: Double)
     *         and the weights Vector could be either a SparseVector or DenseVector
     */
    override def read(json: JsValue): SVMModel = {
      val fields = json.asJsObject.fields
      val intercept = fields.getOrElse("intercept", throw new IllegalArgumentException("Error in de-serialization: Missing intercept."))
        .asInstanceOf[JsNumber].value.doubleValue()

      val weights = fields.get("weights").map(v => {
        VectorFormat.read(v)
      }
      ).get

      new SVMModel(weights, intercept)
    }

  }

  implicit object NaiveBayesModelFormat extends JsonFormat[NaiveBayesModel] {

    override def write(obj: NaiveBayesModel): JsValue = {
      JsObject(
        "labels" -> obj.labels.toJson,
        "pi" -> obj.pi.toJson,
        "theta" -> obj.theta.toJson
      )
    }

    override def read(json: JsValue): NaiveBayesModel = {
      val fields = json.asJsObject.fields
      val labels = getOrInvalid(fields, "labels").convertTo[Array[Double]]
      val pi = getOrInvalid(fields, "pi").convertTo[Array[Double]]
      val theta = getOrInvalid(fields, "theta").convertTo[Array[Array[Double]]]
      new NaiveBayesModel(labels, pi, theta)
    }

  }

  implicit object PrincipalComponentsModelFormat extends JsonFormat[PrincipalComponentsData] {

    override def write(obj: PrincipalComponentsData): JsValue = {
      val singularValues = VectorFormat.write(obj.singularValues)
      val meanVector = VectorFormat.write(obj.meanVector)
      JsObject(
        "k" -> obj.k.toJson,
        "observationColumns" -> obj.observationColumns.toJson,
        "meanCentered" -> obj.meanCentered.toJson,
        "meanVector" -> meanVector,
        "singularValues" -> singularValues,
        "vFactor" -> obj.vFactor.toJson
      )
    }

    override def read(json: JsValue): PrincipalComponentsData = {
      val fields = json.asJsObject.fields
      val k = getOrInvalid(fields, "k").convertTo[Int]
      val observationColumns = getOrInvalid(fields, "observationColumns").convertTo[List[String]]

      val meanCentered = getOrInvalid(fields, "meanCentered").convertTo[Boolean]

      val meanVector = VectorFormat.read(getOrInvalid(fields, "meanVector"))

      val singularValues = VectorFormat.read(getOrInvalid(fields, "singularValues"))

      val vFactor = MatrixFormat.read(getOrInvalid(fields, "vFactor"))

      new PrincipalComponentsData(k, observationColumns, meanCentered, meanVector, singularValues, vFactor)
    }
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

  implicit val logRegDataFormat = jsonFormat2(LogisticRegressionData)
  implicit val classficationWithSGDTrainFormat = jsonFormat10(ClassificationWithSGDTrainArgs)
  implicit val classificationWithSGDPredictFormat = jsonFormat3(ClassificationWithSGDPredictArgs)
  implicit val classificationWithSGDTestFormat = jsonFormat4(ClassificationWithSGDTestArgs)
  implicit val svmDataFormat = jsonFormat2(SVMData)
  implicit val kmeansDataFormat = jsonFormat3(KMeansData)
  implicit val kmeansModelTrainReturnFormat = jsonFormat2(KMeansTrainReturn)
  implicit val kmeansModelLoadFormat = jsonFormat8(KMeansTrainArgs)
  implicit val kmeansModelPredictFormat = jsonFormat3(KMeansPredictArgs)
  implicit val linRegDataFormat = jsonFormat2(LinearRegressionData)
  implicit val naiveBayesDataFormat = jsonFormat2(NaiveBayesData)
  implicit val naiveBayesTrainFormat = jsonFormat5(NaiveBayesTrainArgs)
  implicit val naiveBayesPredictFormat = jsonFormat3(NaiveBayesPredictArgs)
  implicit val logRegTrainFormat = jsonFormat18(LogisticRegressionTrainArgs)
  implicit val logRegTrainResultsFormat = jsonFormat8(LogisticRegressionSummaryTable)
  implicit val pcaPredictFormat = jsonFormat7(PrincipalComponentsPredictArgs)
  implicit val pcaTrainFormat = jsonFormat5(PrincipalComponentsTrainArgs)
  implicit val pcaTrainReturnFormat = jsonFormat6(PrincipalComponentsTrainReturn)
  implicit val modelPublishFormat = jsonFormat3(ModelPublishArgs)
}

class InvalidJsonException(message: String) extends RuntimeException(message)
