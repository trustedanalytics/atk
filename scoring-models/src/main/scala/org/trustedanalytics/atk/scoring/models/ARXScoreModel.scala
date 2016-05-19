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

import org.trustedanalytics.atk.scoring.interfaces.{ ModelMetaDataArgs, Field, Model }

import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg._
import com.cloudera.sparkts.models.ARXModel

class ARXScoreModel(arxModel: ARXModel, arxData: ARXData) extends ARXModel(arxModel.c, arxModel.coefficients, arxModel.yMaxLag, arxModel.xMaxLag, true) with Model {

  override def score(data: Array[Any]): Array[Any] = {
    val xColumnsLength = arxData.xColumns.length

    if (data.length == 0)
      throw new IllegalArgumentException("Unable to score using ARX model, because the array of data passed in is empty.")

    var predictedValues = Array[Any]()

    // We should have an array of y values, and an array of x values
    if (data.length != 2)
      throw new IllegalArgumentException("Expected 2 arrays of data (for y values and x values), but received " +
        data.length.toString + " items.")

    if (!data(0).isInstanceOf[List[Double]])
      throw new IllegalArgumentException("Expected first element in data array to be an List[Double] of y values.")

    if (!data(1).isInstanceOf[List[Double]])
      throw new IllegalArgumentException("Expected second element in data array to be an List[Double] of x values.")

    val yValues = new DenseVector(data(0).asInstanceOf[List[Double]].map(ScoringModelUtils.asDouble(_)).toArray)
    val xArray = data(1).asInstanceOf[List[Double]].map(ScoringModelUtils.asDouble(_)).toArray

    if (xArray.length != (yValues.length * xColumnsLength))
      throw new IllegalArgumentException("Expected " + (yValues.length * xColumnsLength) + " x values, but received " +
        xArray.length.toString)

    val xValues = new DenseMatrix(rows = yValues.length, cols = xColumnsLength, data = xArray)

    data :+ arxModel.predict(yValues, xValues).toArray
  }

  override def input(): Array[Field] = {
    Array[Field](Field("y", "Array[Double]"), Field("x_values", "Array[Double]"))
  }

  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("ARX Model", classOf[ARXModel].getName, classOf[ARXModelReaderPlugin].getName, Map())
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Array[Double]")
  }

}
