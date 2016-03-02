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

package org.trustedanalytics.atk.scoring.models

import org.trustedanalytics.atk.scoring.interfaces.{ ModelMetaDataArgs, Field, Model }

import org.apache.spark.mllib.linalg.Vectors
import breeze.linalg._
import com.cloudera.sparkts.models.ARXModel

class ARXScoreModel(arxModel: ARXModel, arxData: ARXData) extends ARXModel(arxModel.c, arxModel.coefficients, arxModel.yMaxLag, arxModel.xMaxLag, true) with Model {

  override def score(row: Array[Any]): Array[Any] = {
    val xColumnsLength = arxData.xColumns.length

    // Length of the row should be 1 (y) + the number of x columns
    require(row.length == (xColumnsLength + 1), s"Number of items in the row should be ${(xColumnsLength + 1).toString} (1 y value and ${xColumnsLength.toString} x values), but was ${row.length}.")

    // Values should be doubles
    val rowDoubles = row.map(item => {
      ScoringModelUtils.toDouble(item)
    })

    // Create Vector and Matrix to call predict
    val y = new DenseVector(Array[Double](rowDoubles(0)))
    val xValues = rowDoubles.slice(1, rowDoubles.length)
    val x = new DenseMatrix(rows = 1, cols = xColumnsLength, data = xValues)

    arxModel.predict(y, x).map(_.asInstanceOf[Any]).toArray
  }

  override def input(): Array[Field] = {
    arxData.xColumns.map(name => Field(name, "Double")).toArray
  }
  override def modelMetadata(): ModelMetaDataArgs = {
    new ModelMetaDataArgs("ARX Model", classOf[ARXModel].getName, classOf[ARXModelReaderPlugin].getName, Map())
  }

  override def output(): Array[Field] = {
    var output = input()
    output :+ Field("score", "Int")
  }

}
