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

import java.lang

import com.intel.daal.algorithms.kmeans._
import com.intel.daal.data_management.data.HomogenNumericTable
import com.intel.daal.services.DaalContext
import org.trustedanalytics.atk.scoring.interfaces.{ Field, Model, ModelMetaDataArgs }

class DaalKMeansScoreModel(kmeansData: DaalKMeansModelData) extends Model {

  override def score(data: Array[Any]): Array[Any] = {
    val features: Array[Double] = data.map(y => ScoringModelUtils.asDouble(y))
    val clusterId = predictCluster(features)

    data :+ clusterId
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def input(): Array[Field] = {
    var input = Array[Field]()
    val obsCols = kmeansData.observationColumns
    obsCols.foreach { name =>
      input = input :+ Field(name, "Double")
    }
    input
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("Daal KMeans Model", classOf[DaalKMeansScoreModel].getName, classOf[DaalKMeansModelReaderPlugin].getName, Map())
  }

  /**
   * @return fields containing the input names and their datatypes along with the output and its datatype
   */
  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Int")
  }

  /**
   * Predict cluster ID from feature array
   *
   * @param features feature array
   * @return cluster ID
   */
  private def predictCluster(features: Array[Double]): Int = {
    val context = new DaalContext

    val algorithm = new Batch(context, classOf[lang.Double], Method.lloydDense, kmeansData.k.toLong, 1L)
    val input = new HomogenNumericTable(context, features, features.length, 1L)
    val centroids = kmeansData.centroids
    centroids.unpack(context)
    algorithm.input.set(InputId.data, input)
    algorithm.input.set(InputId.inputCentroids, centroids)
    algorithm.parameter.setAssignFlag(true)

    val result = algorithm.compute()

    val assignments = result.get(ResultId.assignments).asInstanceOf[HomogenNumericTable]
    val clusterId = ScoringModelUtils.toDoubleArray(assignments).head
    context.dispose()

    clusterId.toInt
  }
}
