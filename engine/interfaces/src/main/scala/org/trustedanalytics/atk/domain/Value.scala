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

import spray.json.JsonFormat

/**
 * Generic String value that can be used by plugins that return a single String
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class StringValue(value: String)

/**
 * Generic Long value that can be used by plugins that return a single Long
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class LongValue(value: Long)

/**
 * Generic Int value that can be used by plugins that return a single Int
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class IntValue(value: Int)

/**
 * Generic Double value that can be used by plugins that return a single Double
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class DoubleValue(value: Double)

/**
 * Generic boolean value that can be used by plugins that return a Boolean
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class BoolValue(value: Boolean)

/**
 * Generic singleton or list value which is a List, but has a Json serializer such that a singleton
 * is accepted
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class SingletonOrListValue[T](value: List[T])

/**
 * Generic double value that can be used by plugins that return a Double
 * @param value "value" is a special string meaning don't treat this return type like a dictionary
 */
case class VectorValue(value: Vector[Double])
