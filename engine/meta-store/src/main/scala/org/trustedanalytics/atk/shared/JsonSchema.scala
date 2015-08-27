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

package org.trustedanalytics.atk.shared

import java.net.URI

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.event.{ EventContext, EventLogging }
import org.trustedanalytics.atk.domain.frame.{ FrameEntity, FrameReference }
import org.trustedanalytics.atk.domain.graph.GraphReference
import org.trustedanalytics.atk.engine.PluginDocAnnotation
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, PluginDoc }
import spray.json.{ AdditionalFormats, StandardFormats }
import org.trustedanalytics.atk.spray.json._
import org.joda.time.DateTime
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.reflect.ClassTag
import org.trustedanalytics.atk.domain.model.ModelReference

import scala.util.{ Try, Success, Failure }

/**
 * Helper to allow access to spray-json utility so that we can ensure we're
 * accessing case class vals in exactly the same way that it will.
 *
 * It seems that the underlying operations are not thread-safe -- Todd 3/10/2015
 */
private[trustedanalytics] class ProductFormatsAccessor extends CustomProductFormats
    with StandardFormats
    with AdditionalFormats {
  override def extractFieldNames(classManifest: ClassManifest[_]): Array[String] =
    super.extractFieldNames(classManifest)
}

/**
 * Generates `JsonSchema` objects to represent case classes
 */
private[trustedanalytics] object JsonSchemaExtractor {

  def getArgumentsSchema[T](tag: ClassTag[T]): ObjectSchema = {
    getProductSchema(tag, includeDefaultValues = true)
  }

  def getReturnSchema[T](tag: ClassTag[T], description: Option[String]): JsonSchema = {
    val manifest: ClassManifest[T] = tag
    val fieldHelper = new ProductFormatsAccessor()
    fieldHelper.extractFieldNames(manifest)

    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
    val schema = getSchemaForType(typ, description = description)._1
    if (schema == JsonSchema.empty) {
      getProductSchema(tag, includeDefaultValues = false).copy(description = description)
    }
    else {
      schema
    }
  }

  /**
   * Entry point for generating schema information for a case class
   * @param tag extended type information for the given type
   * @tparam T the type for which to generate a JSON schema
   */
  def getProductSchema[T](tag: ClassTag[T], includeDefaultValues: Boolean = false): ObjectSchema = {
    // double check that Spray serialization will work
    val manifest: ClassManifest[T] = tag

    val fieldHelper = new ProductFormatsAccessor()
    fieldHelper.extractFieldNames(manifest)

    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val typ: ru.Type = mirror.classSymbol(tag.runtimeClass).toType
    val members: Array[ru.Symbol] = typ.members.filter(m => !m.isMethod).toArray.reverse
    val func = getFieldSchema(typ)(_, _, _, _)
    val ordered = Array.tabulate(members.length) { i => (members(i), i) }
    val defaultValues = if (includeDefaultValues) { getDefaultValuesForClass(tag) } else { List() }
    val propertyInfo = ordered.map({
      case (sym, i) =>
        val annotationArgValues: Option[List[Any]] = sym.annotations.find(a => a.tpe <:< ru.typeOf[ArgDoc]) match {
          case Some(argDocAnnotation) =>
            Some(argDocAnnotation.scalaArgs.map(annotationArg => annotationArg.productElement(0)))
          case _ => None
        }
        val description: Option[String] = annotationArgValues match {
          case Some(argValues) =>
            argValues(0) match {
              case Constant(s: String) => Some(s)
              case _ => throw new RuntimeException("Internal Error - bad @ArgDoc annotation description argument type for $tag (expected String)")
              /*
 *            case _ => throw new RuntimeException("Internal Error - bad Argument description annotation type (expected String)")
 */
            }
          case _ => None
        }

        val tryDefaultValue: Option[Any] = defaultValues.lift(i) match { // see if default value is available
          case None => None
          case Some(null) => None
          case Some(x) => Some(x)
        }
        val optional = tryDefaultValue.isDefined // capture the fact that value was or wasn't available
        val defaultValue: Option[Any] = tryDefaultValue match {
          case None => None
          case Some(value) => value match {
            case None => None
            case Some(x) => Some(x)
            case y => Some(y)
          }
        }
        val (name, (schema, isTypeOption)) = JsonPropertyNameConverter.camelCaseToUnderscores(sym.name.decoded) -> func(sym, Some(i), description, defaultValue)
        // todo: turn this check on to print out offenders, and fix them
        //if (isTypeOption && !optional) {
        //  println(s"WARNING - Argument ${sym.name} in $typ is type Option with no default value")
        //}
        // then use this line...
        //JsonPropertyNameConverter.camelCaseToUnderscores(sym.name.decoded) -> (func(sym, i, description, defaultValue), optional)
        (name, (schema, isTypeOption || optional))
    })

    val required = propertyInfo.filter { case (name, (_, optional)) => !optional }.map { case (n, _) => n }
    val properties = propertyInfo.map { case (name, (schema, _)) => name -> schema }.toMap
    Try {
      ObjectSchema(properties = Some(properties),
        required = Some(required),
        order = Some(members.map(sym => JsonPropertyNameConverter.camelCaseToUnderscores(sym.name.decoded))))
    } match {
      case Success(schema) => schema
      case Failure(ex) => throw new RuntimeException(s"Problem generating object schema for $tag: $ex")
    }
  }

  def getPluginDocAnnotation(tag: ClassTag[_]): Option[PluginDoc] = {
    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val sym: ru.Symbol = mirror.classSymbol(tag.runtimeClass)
    val annotationArgValues: Option[List[Any]] = sym.annotations.find(a => a.tpe <:< ru.typeOf[PluginDoc]) match {
      case Some(argDocAnnotation) =>
        Some(argDocAnnotation.scalaArgs.map(annotationArg => annotationArg.productElement(0)))
      case _ => None
    }
    annotationArgValues match {
      case Some(argValues) =>
        val oneLine: String = argValues(0) match {
          case Constant(s: String) => s
          case _ => throw new RuntimeException("Internal Error - bad oneLine description in annotation (expected String)")
        }
        val extended: String = argValues(1) match {
          case Constant(s: String) => s
          case _ => throw new RuntimeException("Internal Error - bad extended description in annotation (expected String)")
        }
        val returns: String = argValues(2) match {
          case Constant(s: String) => s
          case _ => ""
        }
        Some(PluginDocAnnotation(oneLine, extended, returns))
      case _ => None
    }
  }

  /**
   * Get the default values for a class
   * (adapted from http://stackoverflow.com/questions/14034142/how-do-i-access-default-parameter-values-via-scala-reflection)
   * @param tag class's ClassTag
   * @return list of values.  By convention, null means no default found
   */
  def getDefaultValuesForClass(tag: ClassTag[_]): List[Any] = {
    val mirror = ru.runtimeMirror(tag.runtimeClass.getClassLoader)
    val sym: ru.Symbol = mirror.classSymbol(tag.runtimeClass)
    val module = sym.companionSymbol.asModule
    val im: InstanceMirror = mirror.reflect(mirror.reflectModule(module).instance)
    getDefaultValuesForMethod(im, "apply") // the "apply" method of the case class provides the right signature
  }

  /**
   * Helper method, obtains default values from an instance mirror and method name
   * @param im module instance mirror
   * @param methodName name of method
   * @return list of values.  By convention, null means no default found
   */
  private def getDefaultValuesForMethod(im: InstanceMirror, methodName: String): List[Any] = {
    val ts = im.symbol.typeSignature
    val method = ts.member(newTermName(methodName)).asMethod

    // if method for obtaining a default is found, call it and return the value, else return null
    def valueFor(p: Symbol, i: Int): Any = {
      val defaultValueMethodName = s"$methodName$$default$$${i + 1}"
      ts.member(newTermName(defaultValueMethodName)) match {
        case NoSymbol => null
        case defaultValueSymbol => im.reflectMethod(defaultValueSymbol.asMethod)()
      }
    }
    (for (ps <- method.paramss; p <- ps) yield p).zipWithIndex.map(p => valueFor(p._1, p._2))
  }

  /**
   * Get the schema for one particular field
   *
   * @param clazz the parent class
   * @param symbol the field
   * @param fieldPosition the numeric (0 based) order of this field in the class
   * @return a pair containing the schema, plus a flag indicating if the field is optional or not
   */
  private def getFieldSchema(clazz: ru.Type)(symbol: ru.Symbol, fieldPosition: Option[Int] = None, description: Option[String] = None, defaultValue: Option[Any] = None): (JsonSchema, Boolean) = { //JsonSchema = { //}, Boolean) = {
    val typeSignature: ru.Type = symbol.typeSignatureIn(clazz)
    val schema = getSchemaForType(typeSignature, fieldPosition, description, defaultValue)
    schema
  }

  /**
   * Returns the schema for a particular type.
   *
   * FrameReference and GraphReference types that appear at position zero are marked
   * as "self" arguments.
   *
   * @param typeSignature the type
   * @param fieldPosition the numeric order of the field within its containing class
   * @return
   */
  def getSchemaForType(typeSignature: ru.Type, fieldPosition: Option[Int] = None, description: Option[String] = None, defaultValue: Option[Any] = None): (JsonSchema, Boolean) = { // JsonSchema = { //}, Boolean) = {
    val schema = typeSignature match {
      case t if t =:= typeTag[URI].tpe => StringSchema(format = Some("uri"), description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[String].tpe => StringSchema(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[Boolean].tpe => JsonSchema.bool(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[Int].tpe => JsonSchema.int(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[Long].tpe => JsonSchema.long(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[Float].tpe => JsonSchema.float(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[Double].tpe => JsonSchema.double(description = description, defaultValue = defaultValue)
      case t if t =:= typeTag[DateTime].tpe => JsonSchema.dateTime
      case t if t =:= typeTag[UnitReturn].tpe => JsonSchema.unit
      case t if t =:= typeTag[FrameReference].tpe =>
        val s = JsonSchema.frame(description, defaultValue)
        if (fieldPosition.getOrElse(-1) == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t =:= typeTag[FrameEntity].tpe => // todo: remove FrameEntity, should never be used in the published API meta data
        val s = JsonSchema.frame(description, defaultValue)
        if (fieldPosition.getOrElse(-1) == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t =:= typeTag[GraphReference].tpe =>
        val s = JsonSchema.graph(description, defaultValue)
        if (fieldPosition.getOrElse(-1) == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t =:= typeTag[ModelReference].tpe =>
        val s = JsonSchema.model(description, defaultValue)
        if (fieldPosition.getOrElse(-1) == 0) {
          s.copy(self = Some(true))
        }
        else s
      case t if t.erasure =:= typeTag[Option[Any]].tpe =>
        val (subSchema, _) = getSchemaForType(t.asInstanceOf[TypeRefApi].args.head, fieldPosition, description, defaultValue)
        subSchema
      //parameterized types need special handling
      case t if t.erasure =:= typeTag[Map[Any, Any]].tpe => ObjectSchema()
      case t if t.erasure =:= typeTag[Seq[Any]].tpe => ArraySchema(description = description, defaultValue = defaultValue)
      case t if t.erasure =:= typeTag[Iterable[Any]].tpe => ArraySchema(description = description, defaultValue = defaultValue)
      case t if t.erasure =:= typeTag[List[Any]].tpe => ArraySchema(description = description, defaultValue = defaultValue)
      //array type system works a little differently
      case t if t.typeConstructor =:= typeTag[Array[Any]].tpe.typeConstructor => ArraySchema(description = description, defaultValue = defaultValue)
      case t => JsonSchema.empty
    }
    //schema
    val isTypeOption = typeSignature.erasure =:= typeTag[Option[Any]].tpe
    (schema, isTypeOption) // TODO: optional is determined by using Option type, instead of if default is provided.  Need to switch this!
  }
}
