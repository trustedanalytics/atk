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
package org.trustedanalytics.atk.domain

import scala.reflect.runtime.universe._

trait SerializableType[T]

/**
 * Serializable type for representing Scala and Java serializable types
 */
object SerializableType {

  implicit def AnyValToSerializableType[T <: AnyVal]: SerializableType[T] = new SerializableType[T] {}

  implicit def SerializableToSerializableType[T <: java.io.Serializable]: SerializableType[T] = new SerializableType[T] {}

}

