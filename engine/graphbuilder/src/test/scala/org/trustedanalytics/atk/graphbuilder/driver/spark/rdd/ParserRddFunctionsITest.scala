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


package org.trustedanalytics.atk.graphbuilder.driver.spark.rdd

import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._
import org.trustedanalytics.atk.graphbuilder.parser.rule.RuleParserDSL._
import org.trustedanalytics.atk.graphbuilder.parser.rule.{ EdgeRule, EdgeRuleParser, VertexRule, VertexRuleParser }
import org.trustedanalytics.atk.graphbuilder.parser.{ ColumnDef, CombinedParser, InputSchema }
import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class ParserRddFunctionsITest extends TestingSparkContextWordSpec with Matchers with MockitoSugar {

  "ParserRDDFunctions" should {

    "support a combined parser (one that goes over the input in one step)" in {

      // Input Data
      val inputRows = List(
        List("1", "{(1)}", "1", "Y", "1", "Y"),
        List("2", "{(1)}", "10", "Y", "2", "Y"),
        List("3", "{(1)}", "11", "Y", "3", "Y"),
        List("4", "{(1),(2)}", "100", "N", "4", "Y"),
        List("5", "{(1)}", "101", "Y", "5", "Y"))

      // Input Schema
      val inputSchema = new InputSchema(List(
        new ColumnDef("cf:number", classOf[String]),
        new ColumnDef("cf:factor", classOf[String]),
        new ColumnDef("binary", classOf[String]),
        new ColumnDef("isPrime", classOf[String]),
        new ColumnDef("reverse", classOf[String]),
        new ColumnDef("isPalindrome", classOf[String])))

      // Parser Configuration
      val vertexRules = List(VertexRule(gbId("cf:number"), List(property("isPrime"))), VertexRule(gbId("reverse")))
      val edgeRules = List(EdgeRule(gbId("cf:number"), gbId("reverse"), constant("reverseOf")))

      val vertexParser = new VertexRuleParser(inputSchema, vertexRules)
      val edgeParser = new EdgeRuleParser(inputSchema, edgeRules)
      val parser = new CombinedParser(inputSchema, vertexParser, edgeParser)

      // Setup data in Spark
      val inputRdd = sparkContext.parallelize(inputRows.asInstanceOf[Seq[_]]).asInstanceOf[RDD[Seq[_]]]

      // invoke method under test
      val outputRdd = inputRdd.parse(parser)

      // verification
      outputRdd.count() shouldBe 15
      outputRdd.filterVertices().count() shouldBe 10
      outputRdd.filterEdges().count() shouldBe 5
    }
  }

}
