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

abstract class Naming(val term: String) {

  /**
   * Validates the given name according to standard naming rules.  If no name is given,
   */
  def validate(name: String): String = Naming.validateAlphaNumericUnderscore(name)

  /**
   * Validates the given name according to standard naming rules.  If no name is given,
   * then a unique name is generated, with an optional prefix replacement
   * @param name name Option
   * @param prefix Optional annotation prefix to replace the default prefix
   * @return original name if provided else a generated unique name
   */
  def validateOrGenerate(name: Option[String], prefix: Option[String] = None): String = Naming.validateAlphaNumericUnderscoreOrGenerate(name, { generate(prefix) })

  /**
   * Automatically generate a unique name for a named object
   *
   * The frame name comprises of an optional prefix (else default for the object is used) and a random uuid
   *
   * @param prefix Optional annotation prefix to replace the default prefix
   * @return Frame name
   */
  def generate(prefix: Option[String] = None): String = Naming.generateName(Some(prefix.getOrElse(term + "_")))
}

/**
 * General user object naming
 */
object Naming {

  implicit class Name(val name: String) {
    Naming.validateAlphaNumericUnderscore(name)
  }

  implicit def nameToString(name: Name): String = name.name

  private lazy val alphaNumericUnderscorePattern = "^[a-zA-Z0-9_]+$".r

  /**
   * Determines whether the given text contains exclusively alphanumeric and underscore chars
   */
  def isAlphaNumericUnderscore(text: String): Boolean = (alphaNumericUnderscorePattern findFirstIn text).nonEmpty

  /**
   * Raises an exception if the given text contains anything but alphanumeric or underscore chars
   * @param text subject
   * @return subject
   */
  def validateAlphaNumericUnderscore(text: String): String = {
    if (!isAlphaNumericUnderscore(text)) {
      throw new IllegalArgumentException(s"Invalid string '$text', only alphanumeric and underscore permitted")
    }
    text
  }

  /**
   * Raises an exception if the given text Option contains anything but alphanumeric or underscore chars
   * If the Option is none, an empty string is returned
   * @param text subject
   * @return subject or empty string
   */
  def validateAlphaNumericUnderscoreOrNone(text: Option[String]): String = {
    text match {
      case Some(name) => validateAlphaNumericUnderscore(name)
      case None => ""
    }
  }

  /**
   * Raises an exception if the given text Option contains anything but alphanumeric or underscore chars
   * If the Option is none, an empty string is returned
   * @param text subject
   * @return subject or empty string
   */
  def validateAlphaNumericUnderscoreOrGenerate(text: Option[String], generate: => String): Name = {
    text match {
      case Some(name) => validateAlphaNumericUnderscore(name)
      case None => generate
    }
  }

  /**
   * Generates a unique name, w/ optional prefix and suffix
   *
   * @param prefix Optional annotation prefix  (must be alphanumeric or underscore)
   * @param suffix Optional annotation suffix  (must be alphanumeric or underscore)
   * @return generated name
   */
  def generateName(prefix: Option[String] = None, suffix: Option[String] = None): Name = {
    val p = validateAlphaNumericUnderscoreOrNone(prefix)
    val s = validateAlphaNumericUnderscoreOrNone(suffix)
    p + java.util.UUID.randomUUID().toString.filterNot(_ == '-') + s
  }
}
