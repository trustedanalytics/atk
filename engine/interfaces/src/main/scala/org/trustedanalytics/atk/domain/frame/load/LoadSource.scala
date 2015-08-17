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

package org.trustedanalytics.atk.domain.frame.load

import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{ Column, FrameSchema, Schema }
import org.trustedanalytics.atk.engine.plugin.{ ArgDoc, Invocation }

/**
 * The case classes in this file are used to parse the json submitted as part of a load or append call
 */

/**
 * Object used for parsing and then executing the frame.append command
 *
 */
case class LoadFrameArgs(@ArgDoc("""DataFrame to load data into.
Should be either a uri or id.""") destination: FrameReference,
                         @ArgDoc("""Object describing the data to load into the destination.
Includes the Where and How of loading.""") source: LoadSource)

/**
 * Describes a resource that should be loaded into a DataFrame
 *
 */
case class LoadSource(
    @ArgDoc("""Source object that can be parsed into an RDD. Such as "file" or "frame".""") sourceType: String,
    @ArgDoc("""Location of data to load. Should be appropriate for the source_type.""") uri: String,
    @ArgDoc("""Object describing how to parse the resource. If data already an RDD can be set to None.""") parser: Option[LineParser] = None,
    @ArgDoc("""<TBD>""") data: Option[List[List[Any]]] = None,
    @ArgDoc("""<TBD>""") startTag: Option[List[String]] = None,
    @ArgDoc("""<TBD>""") endTag: Option[List[String]] = None) {

  require(sourceType != null, "sourceType cannot be null")
  require(sourceType == "frame" || sourceType == "file" || sourceType == "hivedb" || sourceType == "strings" || sourceType == "linefile" || sourceType == "multilinefile" || sourceType == "xmlfile",
    "sourceType must be a valid type")
  require(uri != null, "uri cannot be null")
  require(parser != null, "parser cannot be null")
  if (sourceType == "frame" || sourceType == "file" || sourceType == "linefile" || sourceType == "multilinefile") {
    require(data.isEmpty, "if this is not a strings file the data must be None")
  }
  if (sourceType == "strings") {
    require(data.isDefined, "if the sourceType is strings data must not be None")
  }
  if (sourceType == "multilinefile" || sourceType == "xmlfile") {
    require(startTag.isDefined && endTag.isDefined, "if this is a multiline file the start and end tags must be set")
  }

  /**
   * True if source is an existing Frame
   */
  def isFrame: Boolean = {
    sourceType == "frame"
  }

  /**
   * True if source is a pandas Data Frame
   */
  def isClientData: Boolean = {
    sourceType == "strings"
  }

  /**
   * True if source is a file
   */
  def isFieldDelimited: Boolean = {
    sourceType == "file"
  }

  /**
   * True if source is a Line File
   */
  def isFile: Boolean = {
    sourceType == "linefile"
  }

  def isMultilineFile: Boolean = {
    sourceType == "multilinefile" || sourceType == "xmlfile"
  }

  def isHiveDb: Boolean = {
    sourceType == "hivedb"
  }
}

/**
 *  Describes a Parser
 *
 * @param name Parser name such as  builtin/line/separator
 * @param arguments values necessary for initializing the Parser
 */
case class LineParser(name: String, arguments: LineParserArguments)

/**
 * Values needed for initializing a parser.
 *
 * @param separator Char Separator of a delimated file
 * @param schema Schema of Row created in file
 * @param skip_rows number of lines to skip in the file
 */
case class LineParserArguments(separator: Char, schema: SchemaArgs, skip_rows: Option[Int]) {
  skip_rows match {
    case e: Some[Int] => require(skip_rows.get >= 0, "value for skip_header_lines cannot be negative")
    case _ =>
  }
}

/**
 * Schema arguments for the LineParserArguments -
 * these are arguments supplied by the user rather than our own internal schema representation.
 */
case class SchemaArgs(columns: List[(String, DataType)]) {

  /**
   * Convert args to our internal format
   */
  def schema: Schema = {
    new FrameSchema(columns.map { case (name: String, dataType: DataType) => Column(name, dataType) })
  }
}
