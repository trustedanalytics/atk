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

package org.trustedanalytics.atk.graphbuilder.util

/**
 * Convert Primitives to their Object equivalents
 *
 * e.g. classOf[scala.Int] to classOf[java.lang.Integer]
 *      classOf[scala.Long] to classOf[java.lang.Long]
 *      classOf[scala.Char] to classOf[java.lang.Char]
 *      etc.
 *
 * Titan doesn't support primitive properties so we convert them to their Object equivalents.
 *
 * Spark also has trouble de-serializing classOf[Int] because of the underlying Java classes it uses.
 */
object PrimitiveConverter {

  // Stable values are needed for pattern matching primitive classes
  // See http://stackoverflow.com/questions/7157143/how-can-i-match-classes-in-a-scala-match-statement
  private val int = classOf[Int]
  private val long = classOf[Long]
  private val float = classOf[Float]
  private val double = classOf[Double]
  private val byte = classOf[Byte]
  private val short = classOf[Short]
  private val boolean = classOf[Boolean]
  private val char = classOf[Char]

  /**
   * Convert Primitives to their Object equivalents
   *
   * e.g. classOf[scala.Int] to classOf[java.lang.Integer]
   *      classOf[scala.Long] to classOf[java.lang.Long]
   *      classOf[scala.Char] to classOf[java.lang.Char]
   *      etc.
   *
   * Titan doesn't support primitive properties so we convert them to their Object equivalents.
   *
   * Spark also has trouble de-serializing classOf[Int] because of the underlying Java classes it uses.
   *
   * @param dataType convert primitives to Objects, e.g. classOf[Int] to classOf[java.lang.Integer].
   * @return the dataType unchanged, unless it was a primitive
   */
  def primitivesToObjects(dataType: Class[_]): Class[_] = dataType match {
    case `int` => classOf[java.lang.Integer]
    case `long` => classOf[java.lang.Long]
    case `float` => classOf[java.lang.Float]
    case `double` => classOf[java.lang.Double]
    case `byte` => classOf[java.lang.Byte]
    case `short` => classOf[java.lang.Short]
    case `boolean` => classOf[java.lang.Boolean]
    case `char` => classOf[java.lang.Character]
    case default => default
  }

}
