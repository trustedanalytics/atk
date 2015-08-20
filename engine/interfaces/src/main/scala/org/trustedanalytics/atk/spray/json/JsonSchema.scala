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

package org.trustedanalytics.atk.spray.json

import java.net.URI

import scala.annotation.Annotation

trait JsonSchema {
  def id: Option[URI]
  def title: Option[String]
  //def $schema: Option[String]
  def description: Option[String]
  def `type`: Option[String]
}

object JsonSchema {
  val empty: JsonSchema = new JsonSchema {
    def id = None
    def title = None
    //def $schema = None
    def description = None
    def `type` = None
  }

  val unit = UnitSchema()

  def bool(description: Option[String] = None, defaultValue: Option[Any] = None) =
    BooleanSchema(id = Some(new URI("atk:bool")),
      description = description,
      defaultValue = defaultValue)

  def int(description: Option[String] = None, defaultValue: Option[Any] = None) =
    NumberSchema(id = Some(new URI("atk:int32")),
      description = description,
      defaultValue = defaultValue,
      minimum = Some(Int.MinValue),
      maximum = Some(Int.MaxValue),
      multipleOf = Some(1.0))

  def long(description: Option[String] = None, defaultValue: Option[Any] = None) =
    NumberSchema(id = Some(new URI("atk:int64")),
      description = description,
      defaultValue = defaultValue,
      minimum = Some(Long.MinValue),
      maximum = Some(Long.MaxValue),
      multipleOf = Some(1.0))

  def float(description: Option[String] = None, defaultValue: Option[Any] = None) =
    NumberSchema(id = Some(new URI("atk:float32")),
      description = description,
      defaultValue = defaultValue,
      minimum = Some(Float.MinValue),
      maximum = Some(Float.MaxValue))

  def double(description: Option[String] = None, defaultValue: Option[Any] = None) =
    NumberSchema(id = Some(new URI("atk:float64")),
      description = description,
      defaultValue = defaultValue,
      minimum = Some(Double.MinValue),
      maximum = Some(Double.MaxValue))

  val dateTime = StringSchema(format = Some("date-time"))

  def entityReference(idUri: String, description: Option[String], defaultValue: Option[Any]) =
    StringSchema(id = Some(new URI(idUri)),
      description = description,
      defaultValue = defaultValue,
      format = Some("uri/entity"))

  def frame(description: Option[String] = None, defaultValue: Option[Any] = None) = entityReference("atk:frame", description, defaultValue)

  def graph(description: Option[String] = None, defaultValue: Option[Any] = None) = entityReference("atk:graph", description, defaultValue)

  def model(description: Option[String] = None, defaultValue: Option[Any] = None) = entityReference("atk:model", description, defaultValue)

  def vector(length: Long, description: Option[String] = None, defaultValue: Option[Any] = None) =
    ArraySchema(id = Some(new URI("atk:vector")),
      description = description,
      defaultValue = defaultValue,
      `type` = Some(s"vector($length)"))

}

sealed trait Primitive extends JsonSchema {
  //def $schema = None
}

case class DocProperty(description: String) extends Annotation

case class ObjectSchema(
    id: Option[URI] = None,
    title: Option[String] = None,
    //$schema: Option[String] = Some("http://trustedanalytics.com/iat/schema/json-schema-04"),
    description: Option[String] = None,
    defaultValue: Option[Any] = None,
    maxProperties: Option[Int] = None,
    minProperties: Option[Int] = None,
    required: Option[Array[String]] = None,
    additionalProperties: Option[Boolean] = None,
    properties: Option[Map[String, JsonSchema]] = None,
    patternProperties: Option[Map[String, JsonSchema]] = None,
    definitions: Option[Map[String, JsonSchema]] = None,
    order: Option[Array[String]] = None,
    `type`: Option[String] = Some("object")) extends JsonSchema {

  // todo: enable this...
  //  if (required.isDefined && order.isDefined && !order.get.startsWith(required.get)) {
  //    throw new RuntimeException("Bad signature found -- all optional arguments must be positioned at the end of the argument list")
  //    // there's nothing really here to give the user in a message to identify the object, best to catch above
  //  }
}

case class StringSchema(
    id: Option[URI] = None,
    title: Option[String] = None,
    description: Option[String] = None,
    defaultValue: Option[Any] = None,
    maxLength: Option[Int] = None,
    minLength: Option[Int] = None,
    pattern: Option[String] = None,
    format: Option[String] = None,
    self: Option[Boolean] = None,
    `type`: Option[String] = Some("string")) extends Primitive {
  require(maxLength.isEmpty || maxLength.get > 0, "maxLength must be greater than zero")
  require(minLength.isEmpty || maxLength.get >= 0, "minLength must be greater than or equal to zero")
  require(minLength.getOrElse(0) <= maxLength.getOrElse(Int.MaxValue), "maximum must be at least equal to minimum")
}

case class ArraySchema(id: Option[URI] = None,
                       title: Option[String] = None,
                       description: Option[String] = None,
                       defaultValue: Option[Any] = None,
                       additionalItems: Option[Either[Boolean, ObjectSchema]] = None,
                       items: Option[Either[ObjectSchema, Array[ObjectSchema]]] = None,
                       maxItems: Option[Int] = None,
                       minItems: Option[Int] = None,
                       uniqueItems: Option[Boolean] = None,
                       `type`: Option[String] = Some("array")) extends Primitive {
  require(maxItems.isEmpty || maxItems.get >= 0, "maxItems may not be less than zero")
  require(minItems.isEmpty || minItems.get >= 0, "minItems may not be less than zero")
}

case class NumberSchema(id: Option[URI] = None,
                        title: Option[String] = None,
                        description: Option[String] = None,
                        defaultValue: Option[Any] = None,
                        minimum: Option[Double] = None,
                        exclusiveMinimum: Option[Double] = None,
                        maximum: Option[Double] = None,
                        exclusiveMaximum: Option[Double] = None,
                        multipleOf: Option[Double] = None,
                        `type`: Option[String] = Some("number")) extends Primitive {
}

case class BooleanSchema(id: Option[URI] = None,
                         title: Option[String] = None,
                         description: Option[String] = None,
                         defaultValue: Option[Any] = None,
                         `type`: Option[String] = Some("boolean")) extends Primitive {
}

case class UnitSchema(id: Option[URI] = None,
                      title: Option[String] = None,
                      description: Option[String] = None,
                      `type`: Option[String] = Some("unit")) extends Primitive {}

