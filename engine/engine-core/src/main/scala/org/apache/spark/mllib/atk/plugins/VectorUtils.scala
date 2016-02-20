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
package org.apache.spark.mllib.atk.plugins

import org.apache.spark.mllib.linalg.Vector
import scala.collection.JavaConversions._

/**
 * Utility methods for converting between different kinds of vectors
 */
object VectorUtils {

  def toMahoutVector(mllibVector: Vector): org.apache.mahout.math.DenseVector = {
    val mllibArray = mllibVector.toArray
    new org.apache.mahout.math.DenseVector(mllibArray)
  }

  def toScalaVector(mahoutVector: org.apache.mahout.math.Vector): scala.collection.immutable.Vector[Double] = {
    mahoutVector.all().iterator().map(element => element.get()).toVector
  }

  def toDoubleArray(mahoutVector: org.apache.mahout.math.Vector): Array[Double] = {
    toScalaVector(mahoutVector).toArray
  }

}
