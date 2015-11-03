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

package org.trustedanalytics.atk.graphbuilder.parser.rule

import org.trustedanalytics.atk.graphbuilder.elements.Property
import org.trustedanalytics.atk.graphbuilder.parser._

/**
 * Parse zero or more properties using a list of rules
 */
case class PropertyRuleParser(propertyRules: Seq[PropertyRule]) extends Serializable {

  // create a parser for each rule
  val propertyParsers = propertyRules.map(rule => rule -> new SinglePropertyRuleParser(rule)).toMap

  /**
   * Parser zero or more properties from the supplied input using the configured rules.
   */
  def parse(row: InputRow): Set[Property] = {
    (for {
      rule <- propertyRules
      if rule appliesTo row
    } yield propertyParsers(rule).parse(row)).toSet
  }

}

/**
 * Always parse a single property using a single rule.
 *
 * This parser should be used for GbId's.
 */
private[graphbuilder] case class SinglePropertyRuleParser(rule: PropertyRule) extends Serializable {

  /**
   * Always parse a singe property from the supplied input using the configured rule.
   */
  def parse(row: InputRow): Property = {
    new Property(rule.key.value(row), rule.value.value(row))
  }
}
