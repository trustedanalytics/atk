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

package org.apache.spark.mllib

import com.cloudera.sparkts.models.{ ARIMAModel, ARXModel }
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import libsvm.svm_model
import org.apache.spark.mllib.classification.{ NaiveBayesModel, SVMModel }
import org.apache.spark.mllib.clustering.KMeansModel
import com.cloudera.sparkts
import org.apache.spark.mllib.linalg.{ DenseMatrix, DenseVector, Matrix, SparseVector, Vector }
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.{ Algo, FeatureType }
import org.apache.spark.mllib.tree.model._
import org.trustedanalytics.atk.scoring.models._
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.collection.immutable.Map

/**
 * Implicit conversions for Logistic Regression objects to/from JSON
 */
//this module needs to be moved to another place
object ScoringJsonReaderWriters {

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
      val size = getOrInvalid(fields, "size").asInstanceOf[JsNumber].value.intValue
      val indices = getOrInvalid(fields, "indices").asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue).toArray
      val values = getOrInvalid(fields, "values").asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray

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
      val values = getOrInvalid(fields, "values").asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray
      new DenseVector(values)
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
        "num_rows" -> JsNumber(obj.numRows),
        "num_cols" -> JsNumber(obj.numCols),
        "values" -> new JsArray(obj.values.map(d => JsNumber(d)).toList),
        "is_transposed" -> JsBoolean(obj.isTransposed)
      )
    }

    override def read(json: JsValue): DenseMatrix = {
      val fields = json.asJsObject.fields

      val numRows = getOrInvalid(fields, "num_rows").convertTo[Int]
      val numCols = getOrInvalid(fields, "num_cols").convertTo[Int]
      val values = getOrInvalid(fields, "values").asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue).toArray
      val isTransposed = getOrInvalid(fields, "is_transposed").convertTo[Boolean]

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
      val svmParam = new libsvm.svm_parameter()
      val fields = json.asJsObject.fields
      svmParam.svm_type = fields.get("svm_type").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.kernel_type = fields.get("kernel_type").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.degree = fields.get("degree").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.gamma = fields.get("gamma").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.coef0 = fields.get("coef0").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.cache_size = fields.get("cache_size").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.eps = fields.get("eps").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.C = fields.get("C").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.nr_weight = fields.get("nr_weight").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.nu = fields.get("nu").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.p = fields.get("p").get.asInstanceOf[JsNumber].value.doubleValue()
      svmParam.shrinking = fields.get("shrinking").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.probability = fields.get("probability").get.asInstanceOf[JsNumber].value.intValue()
      svmParam.weight_label = fields.get("weight_label").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      svmParam.weight = fields.get("weight").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray

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
        case x => new JsArray(x.map(i => JsNumber(i)).toList)
      }
      val checkProbA = obj.probA match {
        case null => JsNull
        case x => new JsArray(x.map(d => JsNumber(d)).toList)
      }
      val checkProbB = obj.probB match {
        case null => JsNull
        case x => new JsArray(x.map(d => JsNumber(d)).toList)
      }
      val checkNsv = obj.nSV match {
        case null => JsNull
        case x => new JsArray(x.map(d => JsNumber(d)).toList)
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
        case x => x.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      }
      val probB = fields.get("probB").get match {
        case JsNull => null
        case x => x.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.doubleValue()).toArray
      }
      val sv_indices = fields.get("sv_indices").get.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      val sv_coef = fields.get("sv_coef").get.asInstanceOf[JsArray].elements.map(row => row.asInstanceOf[JsArray].elements.map(j => j.asInstanceOf[JsNumber].value.doubleValue()).toArray).toArray
      val label = fields.get("label").get match {
        case JsNull => null
        case x => x.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
      }
      val nSV = fields.get("nSV").get match {
        case JsNull => null
        case x => x.asInstanceOf[JsArray].elements.map(i => i.asInstanceOf[JsNumber].value.intValue()).toArray
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

  implicit object LibSvmDataFormat extends JsonFormat[LibSvmData] {
    /**
     * The write methods converts from LibSvmData to JsValue
     * @param obj LibSvmData. Where LibSvmData format is:
     *            LibSvmData(svmModel: svm_model, observationColumns: List[String])
     * @return JsValue
     */
    override def write(obj: LibSvmData): JsValue = {
      val model = LibSVMModelFormat.write(obj.svmModel)
      JsObject("model" -> model,
        "observation_columns" -> obj.observationColumns.toJson)
    }

    /**
     * The read method reads a JsValue to LibSvmData
     * @param json JsValue
     * @return LibSvmData with format LibSvmData(svmModel: svm_model, observationColumns: List[String])
     */
    override def read(json: JsValue): LibSvmData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val model = fields.get("svm_model").map(v => {
        LibSVMModelFormat.read(v)
      }
      ).get
      new LibSvmData(model, obsCols)
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

  implicit object KmeansDataFormat extends JsonFormat[KMeansData] {
    /**
     * The write methods converts from KMeansData to JsValue
     * @param obj KMeansData. Where KMeansData format is:
     *            KMeansData(kMeansModel: KMeansModel, observationColumns: List[String], columnScalings: List[Double])
     * @return JsValue
     */
    override def write(obj: KMeansData): JsValue = {
      val model = KmeansModelFormat.write(obj.kMeansModel)
      JsObject("k_means_model" -> model,
        "observation_columns" -> obj.observationColumns.toJson,
        "column_scalings" -> obj.columnScalings.toJson)
    }

    /**
     * The read method reads a JsValue to KMeansData
     * @param json JsValue
     * @return KMeansData with format KMeansData(kMeansModel: KMeansModel, observationColumns: List[String], columnScalings: List[Double])
     */
    override def read(json: JsValue): KMeansData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val colScales = getOrInvalid(fields, "column_scalings").convertTo[List[Double]]
      val model = fields.get("k_means_model").map(v => {
        KmeansModelFormat.read(v)
      }
      ).get
      new KMeansData(model, obsCols, colScales)
    }
  }

  implicit object DaalKMeansModelDataFormat extends JsonFormat[DaalKMeansModelData] {
    /**
     * The write methods converts from DaalKMeansModelData to JsValue
     * @param obj DaalKMeansModelData. Where DaalKMeansModelData format is:
     *            KMeansData(observationColumns: List[String], labelColumn: String,
     *            centroids: HomogenNumericTable, k: Int, columnScalings: Option[List[Double])
     * @return JsValue
     */
    override def write(obj: DaalKMeansModelData): JsValue = {
      val centroidsMatrix = ScoringModelUtils.toArrayOfDoubleArray(obj.centroids)

      JsObject(
        "observation_columns" -> obj.observationColumns.toJson,
        "label_column" -> obj.labelColumn.toJson,
        "centroids" -> centroidsMatrix.toJson,
        "k" -> obj.k.toJson,
        "column_scalings" -> obj.columnScalings.toJson)
    }

    /**
     * The read method reads a JsValue to DaalKMeansModelData
     * @param json JsValue
     * @return DaalKMeansModelData. Where DaalKMeansModelData format is:
     *            KMeansData(observationColumns: List[String], labelColumn: String,
     *            centroids: HomogenNumericTable, k: Int, columnScalings: Option[List[Double])
     */
    override def read(json: JsValue): DaalKMeansModelData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val labelCol = getOrInvalid(fields, "label_column").convertTo[String]
      val k = getOrInvalid(fields, "k").convertTo[Int]

      val colScales = fields.get("column_scalings") match {
        case Some(scaling) => Some(scaling.convertTo[List[Double]])
        case _ => None
      }

      val centroidsMatrix = getOrInvalid(fields, "centroids").convertTo[Array[Array[Double]]]
      val centroids = ScoringModelUtils.toDaalNumericTable(centroidsMatrix)

      new DaalKMeansModelData(obsCols, labelCol, centroids, k, colScales)
    }
  }

  implicit object DaalLinearRegressionDataFormat extends JsonFormat[DaalLinearRegressionModelData] {
    /**
     * The write methods converts from DaalLinearRegressionModelData to JsValue
     * @param obj DaalLinearRegressionModelData. Where DaalLinearRegressionModelData format is:
     *            DaalLinearRegressionModelData(serializedModel: List[Byte], observationColumns: List[String],
     *                                          valueColumn: String, weights: Array[Double], intercept: Double)
     * @return JsValue
     */
    override def write(obj: DaalLinearRegressionModelData): JsValue = {
      JsObject(
        "serialized_model" -> obj.serializedModel.toJson,
        "observation_columns" -> obj.observationColumns.toJson,
        "value_column" -> obj.valueColumn.toJson,
        "weights" -> obj.weights.toJson,
        "intercept" -> obj.intercept.toJson
      )
    }

    /**
     * The read method reads a JsValue to LinearRegressionData
     * @param json JsValue
     * @return DaalLinearRegressionModelData with format DaalLinearRegressionModelData(serializedModel: List[Byte],
     *                                          observationColumns: List[String], valueColumn: String,
     *                                          weights: Array[Double], intercept: Double)
     */
    override def read(json: JsValue): DaalLinearRegressionModelData = {
      val fields = json.asJsObject.fields
      val serializedModel = getOrInvalid(fields, "serialized_model").convertTo[List[Byte]]
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val valueColumn = getOrInvalid(fields, "value_column").convertTo[String]
      val weights = getOrInvalid(fields, "weights").convertTo[Array[Double]]
      val intercept = getOrInvalid(fields, "intercept").convertTo[Double]

      new DaalLinearRegressionModelData(serializedModel, obsCols, valueColumn, weights, intercept)
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

  implicit object SVMDataFormat extends JsonFormat[SVMData] {
    /**
     * The write methods converts from SVMData to JsValue
     * @param obj SVMData. Where SVMData's format is
     *            SVMData(svmModel: SVMModel, observationColumns: List[String])
     * @return JsValue
     */
    override def write(obj: SVMData): JsValue = {
      val model = SVMModelFormat.write(obj.svmModel)
      JsObject("svm_model" -> model,
        "observation_columns" -> obj.observationColumns.toJson)
    }

    /**
     * The read method reads a JsValue to SVMData
     * @param json JsValue
     * @return SVMData with format SVMData(svmModel: SVMModel, observationColumns: List[String])
     */
    override def read(json: JsValue): SVMData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val model = fields.get("svm_model").map(v => {
        SVMModelFormat.read(v)
      }
      ).get
      new SVMData(model, obsCols)
    }
  }

  implicit object PrincipalComponentsModelFormat extends JsonFormat[PrincipalComponentsData] {
    /**
     * The write methods converts from PrincipalComponentsData to JsValue
     * @param obj PrincipalComponentsData. Where PrinicipalComponentData's format is
     *            PrincipalComponentsData(val k: Int, val observationColumns: List[String], meanCentered: Boolean,
     *            meanVector:org.apache.spark.mllib.linalg.Vector, singularValues: org.apache.spark.mllib.linalg.Vector,
     *            vFactor: org.apache.spark.mllib.linalg.Matrix)
     * @return JsValue
     */
    override def write(obj: PrincipalComponentsData): JsValue = {
      val singularValues = VectorFormat.write(obj.singularValues)
      val meanVector = VectorFormat.write(obj.meanVector)
      JsObject(
        "k" -> obj.k.toJson,
        "observation_columns" -> obj.observationColumns.toJson,
        "mean_centered" -> obj.meanCentered.toJson,
        "mean_vector" -> meanVector,
        "singular_values" -> singularValues,
        "v_factor" -> obj.vFactor.toJson
      )
    }

    /**
     * The read methods converts from PrincipalComponentsData to JsValue
     * @param json JsValue
     * @return PrincipalComponentsData(val k: Int, val observationColumns: List[String], meanCentered: Boolean,
     *            meanVector: org.apache.spark.mllib.linalg.Vector, singularValues: org.apache.spark.mllib.linalg.Vector,
     *            vFactor: org.apache.spark.mllib.linalg.Matrix)
     */
    override def read(json: JsValue): PrincipalComponentsData = {
      val fields = json.asJsObject.fields
      val k = getOrInvalid(fields, "k").convertTo[Int]
      val observationColumns = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val meanCentered = getOrInvalid(fields, "mean_centered").convertTo[Boolean]
      val meanVector = VectorFormat.read(getOrInvalid(fields, "mean_vector"))
      val singularValues = VectorFormat.read(getOrInvalid(fields, "singular_values"))
      val vFactor = MatrixFormat.read(getOrInvalid(fields, "v_factor"))

      new PrincipalComponentsData(k, observationColumns, meanCentered, meanVector, singularValues, vFactor)
    }
  }

  implicit object LdaModelFormat extends JsonFormat[LdaModel] {
    /**
     * The write methods converts from LdaModel to JsValue
     * @param obj LdaModel. Where LdaModel's format is
     *            LdaModel(val numTopics: Int,val topicWordMap: Map[String, scala.Vector])
     * @return JsValue
     */
    override def write(obj: LdaModel): JsValue = {
      JsObject(
        "num_topics" -> JsNumber(obj.numTopics),
        "topic_word_map" -> obj.topicWordMap.toJson,
        "document_column" -> obj.documentColumnName.toJson,
        "word_column" -> obj.wordColumnName.toJson
      )
    }

    /**
     * The read methods converts from LdaModel to JsValue
     * @param json JsValue
     * @return LdaModel with format LdaModel(val numTopics: Int,val topicWordMap: Map[String, scala.Vector])
     */
    override def read(json: JsValue): LdaModel = {
      val fields = json.asJsObject.fields
      val numTopics = getOrInvalid(fields, "num_topics").convertTo[Int]
      val topicWordMap = getOrInvalid(fields, "topic_word_map").convertTo[Map[String, scala.Vector[Double]]]
      val documentColumnName = getOrInvalid(fields, "document_column").convertTo[String]
      val wordColumnName = getOrInvalid(fields, "word_column").convertTo[String]

      LdaModel(numTopics, topicWordMap, documentColumnName, wordColumnName)
    }
  }

  implicit object LdaModelPredictReturnFormat extends JsonFormat[LdaModelPredictReturn] {
    /**
     * The write methods converts from LdaModelPredictReturn to JsValue
     * @param obj LdaModelPredictReturn. Where LdaModelPredictReturn's format is
     *            LdaModelPredictReturn(val topicsGivenDoc: scala.Vector,val newWordsCount: Int, val newWordsPercentage: Double)
     * @return JsValue
     */
    override def write(obj: LdaModelPredictReturn): JsValue = {
      JsObject(
        "topics_given_docs" -> obj.topicsGivenDoc.toJson,
        "new_words_count" -> JsNumber(obj.newWordsCount),
        "new_words_percentage" -> JsNumber(obj.newWordsPercentage)
      )
    }

    /**
     * The read methods converts from LdaModelPredictReturn to JsValue
     * @param json JsValue
     * @return LdaModelPredictReturn with format
     *         LdaModelPredictReturn(val topicsGivenDoc: scala.Vector,val newWordsCount: Int, val newWordsPercentage: Double)
     */
    override def read(json: JsValue): LdaModelPredictReturn = {
      val fields = json.asJsObject.fields
      val topicsGivenDoc = getOrInvalid(fields, "topics_given_docs").convertTo[scala.Vector[Double]]
      val newWordsCount = getOrInvalid(fields, "new_words_count").convertTo[Int]
      val newWordsPercentage = getOrInvalid(fields, "new_words_percentage").convertTo[Double]

      LdaModelPredictReturn(topicsGivenDoc, newWordsCount, newWordsPercentage)
    }
  }

  implicit object AlgoFormat extends JsonFormat[Algo] {
    /**
     * The write method converts from MLLib's Algo to JsValue
     * @param obj Algo
     * @return JsValue
     */
    override def write(obj: Algo): JsValue = {
      JsObject("algo" -> obj.toString.toJson)
    }

    /**
     * The read method converts from JsValue to MLLib's Algo
     * @param json JsValue
     * @return Algo
     */
    override def read(json: JsValue): Algo = {
      val fields = json.asJsObject.fields
      val a = getOrInvalid(fields, "algo").convertTo[String]
      Algo.withName(a)
    }
  }

  implicit object FeatureTypeFormat extends JsonFormat[FeatureType] {
    /**
     * The write method converts from MLLib's FeatureType to JsValue
     * @param obj FeatureType
     * @return JsValue
     */
    override def write(obj: FeatureType): JsValue = {
      JsObject("feature_type" -> obj.toString.toJson)
    }

    /**
     * The read method coneverts from JsValue to MLLib's FeatureType
     * @param json JsValue
     * @return FeatureType
     */
    override def read(json: JsValue): FeatureType = {
      val fields = json.asJsObject.fields
      val f = getOrInvalid(fields, "feature_type").convertTo[String]
      FeatureType.withName(f)
    }
  }

  implicit object SplitFormat extends JsonFormat[Split] {
    /**
     * The write method converts from MLLib's Split to JsValue
     * @param obj Split(val feature:Int, val threshold: Double, val featureType: FeatureType, categories: List[Double])
     * @return JsValue
     */
    override def write(obj: Split): JsValue = {
      JsObject("feature" -> obj.feature.toJson,
        "threshold" -> obj.threshold.toJson,
        "feature_type" -> FeatureTypeFormat.write(obj.featureType),
        "categories" -> obj.categories.toJson)
    }

    /**
     * The read method converts from JsValue to MLLib's Split
     * @param json JsValue
     * @return Split(val feature:Int, val threshold: Double, val featureType: FeatureType, categories: List[Double])
     */
    override def read(json: JsValue): Split = {
      val fields = json.asJsObject.fields
      val feature = getOrInvalid(fields, "feature").convertTo[Int]
      val threshold = getOrInvalid(fields, "threshold").convertTo[Double]
      val featureType = FeatureTypeFormat.read(getOrInvalid(fields, "feature_type"))
      val categories = getOrInvalid(fields, "categories").convertTo[List[Double]]
      new Split(feature, threshold, featureType, categories)
    }
  }

  implicit object PredictFormat extends JsonFormat[Predict] {
    /**
     * The write method converts from MLLib's Predict to JsValue
     * @param obj Predict(val predict: Double, val prob: Double)
     * @return JsValue
     */
    override def write(obj: Predict): JsValue = {
      JsObject("predict" -> obj.predict.toJson,
        "prob" -> obj.prob.toJson)
    }

    /**
     * The read method converts from JsValue to MLLib's Predict
     * @param json JsValue
     * @return Predict(val predict: Double, val prob: Double)
     */
    override def read(json: JsValue): Predict = {
      val fields = json.asJsObject.fields
      val predict = getOrInvalid(fields, "predict").convertTo[Double]
      val prob = getOrInvalid(fields, "prob").convertTo[Double]
      new Predict(predict, prob)
    }
  }

  implicit object InformationGainStatsFormat extends JsonFormat[InformationGainStats] {
    /**
     * The write method converts from MLLib's InformationGainStats to JsValue
     * @param obj InformationGainStats(val gain: Double, val impurity: Double, val leftImpurity: Double,
     *            val rightImpurity: Double, val leftPredict: Predict, val rightPredict: Predict)
     * @return JsValue
     */
    override def write(obj: InformationGainStats): JsValue = {
      JsObject("gain" -> obj.gain.toJson,
        "impurity" -> obj.impurity.toJson,
        "left_impurity" -> obj.leftImpurity.toJson,
        "right_impurity" -> obj.rightImpurity.toJson,
        "left_predict" -> PredictFormat.write(obj.leftPredict),
        "right_predict" -> PredictFormat.write(obj.rightPredict)
      )
    }

    /**
     * The read method converts from JsValue to MLLib's InformationGainStats
     * @param json JsValue
     * @return InformationGainStats(val gain: Double, val impurity: Double, val leftImpurity: Double,
     *            val rightImpurity: Double, val leftPredict: Predict, val rightPredict: Predict)
     */
    override def read(json: JsValue): InformationGainStats = {
      val fields = json.asJsObject.fields
      val gain = getOrInvalid(fields, "gain").convertTo[Double]
      val impurity = getOrInvalid(fields, "impurity").convertTo[Double]
      val leftImpurity = getOrInvalid(fields, "left_impurity").convertTo[Double]
      val rightImpurity = getOrInvalid(fields, "right_impurity").convertTo[Double]
      val leftPredict = PredictFormat.read(getOrInvalid(fields, "left_predict"))
      val rightPredict = PredictFormat.read(getOrInvalid(fields, "right_predict"))
      new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, leftPredict, rightPredict)
    }
  }

  implicit object NodeFormat extends JsonFormat[Node] {
    /**
     * The write method converts from MLLib's Node to JsValue
     * @param obj Node(val id: Int, val predict: Predict, val impurity: Double, val isLeaf: Boolean,
     *            val split: Option[Split], val leftNode: Option[Node], val rightNode: Option[Node],
     *            val stats: Option[InformationGainStats])
     * @return JsValue
     */
    override def write(obj: Node): JsValue = {
      JsObject("id" -> obj.id.toJson,
        "predict" -> obj.predict.toJson,
        "impurity" -> obj.impurity.toJson,
        "is_leaf" -> obj.isLeaf.toJson,
        "split" -> obj.split.toJson,
        "left_node" -> obj.leftNode.toJson,
        "right_node" -> obj.rightNode.toJson,
        "stats" -> obj.stats.toJson)
    }

    /**
     * The read method converts from JsValue to MLLib's Node
     * @param json JsValue
     * @return Node(val id: Int, val predict: Predict, val impurity: Double, val isLeaf: Boolean,
     *            val split: Option[Split], val leftNode: Option[Node], val rightNode: Option[Node],
     *            val stats: Option[InformationGainStats])
     */
    override def read(json: JsValue): Node = {

      val fields = json.asJsObject.fields
      val id = getOrInvalid(fields, "id").convertTo[Int]
      val predict = getOrInvalid(fields, "predict").convertTo[Predict]
      val impurity = getOrInvalid(fields, "impurity").convertTo[Double]
      val isLeaf = getOrInvalid(fields, "is_leaf").convertTo[Boolean]
      val split = getOrInvalid(fields, "split").convertTo[Option[Split]]
      val leftNode = getOrInvalid(fields, "left_node").convertTo[Option[Node]]
      val rightNode = getOrInvalid(fields, "right_node").convertTo[Option[Node]]
      val stats = getOrInvalid(fields, "stats").convertTo[Option[InformationGainStats]]

      new Node(id, predict, impurity, isLeaf, split, leftNode, rightNode, stats)
    }
  }

  implicit object DecisionTreeModelFormat extends JsonFormat[DecisionTreeModel] {
    /**
     * The write method converts from MLLib's DecisionTreeModel to JsValue
     * @param obj DecisionTreeModel(val topNode: Node, val algo: Algo)
     * @return JsValue
     */
    override def write(obj: DecisionTreeModel): JsValue = {
      JsObject("top_node" -> NodeFormat.write(obj.topNode),
        "algo" -> AlgoFormat.write(obj.algo))
    }

    /**
     * The read method converts from JsValue to MLLib's DecisionTreeModel
     * @param json JsValue
     * @return DecisionTreeModel(val topNode: Node, val algo: Algo)
     */
    override def read(json: JsValue): DecisionTreeModel = {
      val fields = json.asJsObject.fields
      val topNode = NodeFormat.read(getOrInvalid(fields, "top_node"))
      val algo = AlgoFormat.read(getOrInvalid(fields, "algo"))
      new DecisionTreeModel(topNode, algo)
    }
  }

  implicit object RandomForestModelFormat extends JsonFormat[RandomForestModel] {
    /**
     * The write method converts from MLLib's RandomForestModel to JsValue
     * @param obj RandomForestModel(val algo: Algo, val trees: Array[DecisionTreeModel] )
     * @return JsValue
     */
    override def write(obj: RandomForestModel): JsValue = {
      JsObject("algo" -> AlgoFormat.write(obj.algo),
        "trees" -> new JsArray(obj.trees.map(t => DecisionTreeModelFormat.write(t)).toList))
    }

    /**
     * The read method converts from JsValue to MLLib's RandomForestModel
     * @param json JsValue
     * @return RandomForestModel(val algo: Algo, val trees: Array[DecisionTreeModel] )
     */
    override def read(json: JsValue): RandomForestModel = {
      val fields = json.asJsObject.fields
      val algo = AlgoFormat.read(getOrInvalid(fields, "algo"))
      val trees = getOrInvalid(fields, "trees").asInstanceOf[JsArray].elements.map(i => DecisionTreeModelFormat.read(i)).toArray
      new RandomForestModel(algo, trees)
    }
  }

  implicit object RandomForestDataFormat extends JsonFormat[RandomForestClassifierData] {
    /**
     * The write methods converts from RandomForestClassifierData to JsValue
     * @param obj RandomForestClassifierData. Where RandomForestClassifierData format is:
     *            RandomForestClassifierData(randomForestModel: RandomForestModel, observationColumns: List[String], numClasses: Int)
     * @return JsValue
     */
    override def write(obj: RandomForestClassifierData): JsValue = {
      val model = RandomForestModelFormat.write(obj.randomForestModel)
      JsObject("random_forest_model" -> model,
        "observation_columns" -> obj.observationColumns.toJson,
        "num_classes" -> obj.numClasses.toJson)
    }

    /**
     * The read method reads a JsValue to RandomForestClassifierData
     * @param json JsValue
     * @return RandomForestClassifierData with format RandomForestClassifierData(randomForestModel: RandomForestModel, observationColumns: List[String], numClasses: Int)
     */
    override def read(json: JsValue): RandomForestClassifierData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val numClasses = getOrInvalid(fields, "num_classes").convertTo[Int]
      val model = fields.get("random_forest_model").map(v => {
        RandomForestModelFormat.read(v)
      }
      ).get
      new RandomForestClassifierData(model, obsCols, numClasses)
    }
  }

  implicit object RandomForestRegressorDataFormat extends JsonFormat[RandomForestRegressorData] {
    /**
     * The write methods converts from RandomForestRegressorData to JsValue
     * @param obj RandomForestRegressorData. Where RandomForestRegressorData format is:
     *            RandomForestRegressorData(randomForestModel: RandomForestModel, observationColumns: List[String])
     * @return JsValue
     */
    override def write(obj: RandomForestRegressorData): JsValue = {
      val model = RandomForestModelFormat.write(obj.randomForestModel)
      JsObject("random_forest_model" -> model,
        "observation_columns" -> obj.observationColumns.toJson)
    }

    /**
     * The read method reads a JsValue to RandomForestRegressorData
     * @param json JsValue
     * @return RandomForestRegressorData with format RandomForestRegressorData(randomForestModel: RandomForestModel, observationColumns: List[String])
     */
    override def read(json: JsValue): RandomForestRegressorData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val model = fields.get("random_forest_model").map(v => {
        RandomForestModelFormat.read(v)
      }
      ).get
      new RandomForestRegressorData(model, obsCols)
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

  implicit object NaiveBayesDataFormat extends JsonFormat[NaiveBayesData] {
    /**
     * The write methods converts from NaiveBayesData to JsValue
     * @param obj NaiveBayesData. Where NaiveBayesData format is:
     *            NaiveBayesData(naiveBayesModel: NaiveBayesModel, observationColumns: List[String])
     * @return JsValue
     */
    override def write(obj: NaiveBayesData): JsValue = {
      val model = NaiveBayesModelFormat.write(obj.naiveBayesModel)
      JsObject("naive_bayes_model" -> model,
        "observation_columns" -> obj.observationColumns.toJson)

    }

    /**
     * The read method reads a JsValue to NaiveBayesData
     * @param json JsValue
     * @return NaiveBayesData with format NaiveBayesData(naiveBayesModel: NaiveBayesModel, observationColumns: List[String])
     */
    override def read(json: JsValue): NaiveBayesData = {
      val fields = json.asJsObject.fields
      val obsCols = getOrInvalid(fields, "observation_columns").convertTo[List[String]]
      val model = fields.get("naive_bayes_model").map(v => {
        NaiveBayesModelFormat.read(v)
      }
      ).get
      new NaiveBayesData(model, obsCols)
    }
  }

  def getOrInvalid[T](map: Map[String, T], key: String): T = {
    // throw exception if a programmer made a mistake
    map.getOrElse(key, throw new InvalidJsonException(s"expected key $key was not found in JSON $map"))
  }

  implicit object ARXModelFormat extends JsonFormat[ARXModel] {
    /**
     * The write methods converts from ARXModel to JsValue
     * @param obj ARXModel. Where ARXModel's format is
     *              c : scala.Double
     *              coefficients : scala.Array[scala.Double]
     *              yMaxLag : scala.Int
     *              xMaxLag : scala.Int
     * @return JsValue
     */
    override def write(obj: ARXModel): JsValue = {
      JsObject(
        "c" -> obj.c.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "xMaxLag" -> obj.xMaxLag.toJson,
        "yMaxLag" -> obj.yMaxLag.toJson
      // NOTE: unable to save includesOriginalX parameter
      )
    }

    /**
     * The read method reads a JsValue to ARXModel
     * @param json JsValue
     * @return ARXModel with format
     *            c : scala.Double
     *            coefficients : scala.Array[scala.Double]
     *            yMaxLag : scala.Int
     *            xMaxLag : scala.Int
     */
    override def read(json: JsValue): ARXModel = {
      val fields = json.asJsObject.fields
      val c = getOrInvalid(fields, "c").convertTo[Double]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val xMaxLag = getOrInvalid(fields, "xMaxLag").convertTo[Int]
      val yMaxLag = getOrInvalid(fields, "yMaxLag").convertTo[Int]
      // NOTE: unable to get includesOriginalX - defaulting to true
      new ARXModel(c, coefficients, xMaxLag, yMaxLag, true)
    }

  }

  implicit object ARXDataFormat extends JsonFormat[ARXData] {
    /**
     * The write methods converts from ARXData to JsValue
     * @param obj ARXData. Where ARXData format is:
     *            ARXData(arxModel: ARXModel, xColumns: List[String])
     * @return JsValue
     */
    override def write(obj: ARXData): JsValue = {
      val model = ARXModelFormat.write(obj.arxModel)
      JsObject("arx_model" -> model,
        "x_columns" -> obj.xColumns.toJson)
    }

    /**
     * The read method reads a JsValue to ARXData
     * @param json JsValue
     * @return ARXData with format ARXData(arxModel: ARXModel, xColumns: List[String])
     */
    override def read(json: JsValue): ARXData = {
      val fields = json.asJsObject.fields
      val xCols = getOrInvalid(fields, "x_columns").convertTo[List[String]]
      val model = fields.get("arx_model").map(v => {
        ARXModelFormat.read(v)
      }
      ).get
      new ARXData(model, xCols)
    }
  }

  implicit object ARIMAModelFormat extends JsonFormat[ARIMAModel] {
    /**
     * The write methods converts from ARIMAModel to JsValue
     * @param obj ARIMAModel. Where ARIMAModel's format is
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            hasIntercept : scala.Boolean
     * @return JsValue
     */
    override def write(obj: ARIMAModel): JsValue = {
      JsObject(
        "p" -> obj.p.toJson,
        "d" -> obj.d.toJson,
        "q" -> obj.q.toJson,
        "coefficients" -> obj.coefficients.toJson,
        "hasIntercept" -> obj.hasIntercept.toJson
      )
    }

    /**
     * The read method reads a JsValue to ARIMAModel
     * @param json JsValue
     * @return ARIMAModel with format
     *            p : scala.Int
     *            d : scala.Int
     *            q : scala.Int
     *            coefficients : scala:Array[scala.Double]
     *            hasIntercept : scala.Boolean
     */
    override def read(json: JsValue): ARIMAModel = {
      val fields = json.asJsObject.fields
      val p = getOrInvalid(fields, "p").convertTo[Int]
      val d = getOrInvalid(fields, "d").convertTo[Int]
      val q = getOrInvalid(fields, "q").convertTo[Int]
      val coefficients = getOrInvalid(fields, "coefficients").convertTo[Array[Double]]
      val hasIntercept = getOrInvalid(fields, "hasIntercept").convertTo[Boolean]
      new ARIMAModel(p, d, q, coefficients, hasIntercept)
    }

  }

  implicit object ARIMADataFormat extends JsonFormat[ARIMAData] {
    /**
     * The write methods converts from ARIMAData to JsValue
     * @param obj ARIMAData. Where ARIMAData format is:
     *            ARIMAData(arimaModel: ARIMAModel])
     * @return JsValue
     */
    override def write(obj: ARIMAData): JsValue = {
      val model = ARIMAModelFormat.write(obj.arimaModel)
      JsObject("arima_model" -> model)
    }

    /**
     * The read method reads a JsValue to ARIMAData
     * @param json JsValue
     * @return ARIMAData with format ARIMAData(arimaModel: ARIMAModel)
     */
    override def read(json: JsValue): ARIMAData = {
      val fields = json.asJsObject.fields
      val model = fields.get("arima_model").map(v => {
        ARIMAModelFormat.read(v)
      }
      ).get
      new ARIMAData(model)
    }
  }
}

class InvalidJsonException(message: String) extends RuntimeException(message)
