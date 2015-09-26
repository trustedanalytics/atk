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

package org.trustedanalytics.atk.engine.frame.plugins.load

import java.io.File
import java.nio.charset.Charset

import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin.MultiLineTaggedInputFormat
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec
import org.apache.commons.codec.Charsets
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{ Text, LongWritable }
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfter, Matchers }
import org.trustedanalytics.atk.testutils.{ TestingSparkContextWordSpec, DirectoryUtils }

class MultiLineTaggedInputFormatTest extends TestingSparkContextWordSpec with Matchers with BeforeAndAfter {
  var tmpDir: File = null
  var singleRootFile: File = null
  var jsonFile: File = null
  var recursiveNestingFile: File = null
  var altXMLEndFile: File = null
  var xmlQuoteFile: File = null
  var xmlCommentFile: File = null

  override def beforeAll() {
    super[TestingSparkContextWordSpec].beforeAll()
    tmpDir = DirectoryUtils.createTempDirectory("xmlinputformat-test-dir")
    //create test files
    singleRootFile = createTestFile("singleRoot", """<?xml version="1.0" encoding="UTF-8"?>
                               |        <shapes>
                               |            <square>
                               |                <name>left</name>
                               |                <size>3</size>
                               |            </square>
                               |            <triangle>
                               |                <size>3</size>
                               |            </triangle>
                               |            <square color="blue">
                               |                <name>right</name>
                               |                <size>5</size>
                               |            </square>
                               |        </shapes>""")

    jsonFile = createTestFile("json",
      """{
        |   "num": 1,
        |   "status": "}{}{\\"
        |},
        |{
        |   "num":2,
        |   "status": "\"}{\}"
        |}
      """)
    recursiveNestingFile = createTestFile("recursive",
      """<?xml version="1.0" encoding="UTF-8"?>
      |        <shapes>
      |            <square color="blue">
      |                 <shapes>
      |                   <triangle>
      |                       <size>3</size>
      |                       <shapes>
      |                            <square>
      |                               <name>inner</name>
      |                               <size>2</size>
      |                            </square>
      |                       </shapes>
      |                    </triangle>
      |                 </shapes>
      |                <name>right</name>
      |                <size>5</size>
      |            </square>
      |        </shapes>""")
    altXMLEndFile = createTestFile("altXML", """<?xml version="1.0" encoding="UTF-8"?>
                                                    |        <shapes>
                                                    |            <square>
                                                    |                <name>left</name>
                                                    |                <size>3</size>
                                                    |            </square>
                                                    |            <triangle>
                                                    |                <size>3</size>
                                                    |            </triangle>
                                                    |            <square color="blue" />
                                                    |        </shapes>""")
    xmlQuoteFile = createTestFile("xmlQuote", """<?xml version="1.0" encoding="UTF-8"?>
                                                |        <shapes>
                                                |            <square>
                                                |                <name>left'</name>
                                                |                <size>3"</size>
                                                |            </square>
                                                |            <triangle>
                                                |                <size>3</size>
                                                |            </triangle>
                                                |            <square color="blue</square>" />
                                                |        </shapes>""")
    xmlCommentFile = createTestFile("xmlComment", """<?xml version="1.0" encoding="UTF-8"?>
                                                |        <shapes>
                                                |            <square>
                                                |                <name>left</name>
                                                |                <size>3</size>
                                                |            </square>
                                                |       <!--
                                                |            <square>
                                                |                <name>missing</name>
                                                |                <size>3"</size>
                                                |            </square>
                                                |       -->
                                                |            <square>
                                                |                <name>mid</name>
                                                |                <size>3</size>
                                                |       <!--
                                                |            </square>
                                                |            <square>
                                                |                <name>nonexistent</name>
                                                |                <size>3</size>
                                                |       -->
                                                |            </square>
                                                |            <square>
                                                |                <name>right</name>
                                                |                <size>3</size>
                                                |            </square>
                                                |        </shapes>""")
  }

  def createTestFile(fileName: String, fileText: String): File = {
    val file: File = new File(tmpDir, fileName)
    FileUtils.writeStringToFile(file, fileText.stripMargin, "UTF-8")
    file
  }

  override def afterAll() {
    super[TestingSparkContextWordSpec].afterAll()
    DirectoryUtils.deleteTempDirectory(tmpDir)
  }

  "XmlInputFormat" should {
    "load and split an xml file containing multiple targeted subnodes under a single root node" in {
      val rows: RDD[String] = executeXmlInputFormat(singleRootFile, List("<square>", "<square "), List("</square>"))
      val taken = rows.take(100)
      taken.length should be(2)
      taken(0) should include("left")
      taken(1) should include("right")
    }

    "ignore end tags in attribute values and respect escaped quotes" in {
      val rows: RDD[String] = executeXmlInputFormat(jsonFile, List("{"), List("}"), isXml = false)
      val taken = rows.take(100)
      taken.length should be(2)
      taken(0) should include("1")
      taken(1) should include("2")
    }

    "will handle recursively nested tags by splitting the higheset level value" in {
      val rows: RDD[String] = executeXmlInputFormat(recursiveNestingFile, List("<shapes>", "<shapes "), List("</shapes>"))
      val taken = rows.take(100)
      taken.length should be(1)
    }

    "will handle xml nodes that consist of a start and end tags an empty" in {
      val rows: RDD[String] = executeXmlInputFormat(altXMLEndFile, List("<square>", "<square "), List("</square>"))
      val taken = rows.take(100)
      taken.length should be(2)
      taken(0) should include("left")
      taken(1) should include("blue")
    }

    "xml values should only check for escaped quotes inside of empty element tags" in {
      val rows: RDD[String] = executeXmlInputFormat(xmlQuoteFile, List("<square>", "<square "), List("</square>"))
      val taken = rows.take(100)
      taken.length should be(2)
      taken(0) should include("left")
      taken(1) should include("blue")
    }

    "xml values should ignore values found in comments" in {
      val rows: RDD[String] = executeXmlInputFormat(xmlCommentFile, List("<square>", "<square "), List("</square>"))
      val taken = rows.take(100)
      taken.length should be(3)
      taken(0) should include("left")
      taken(1) should include("mid")
      taken(2) should include("right")
    }

  }

  def executeXmlInputFormat(file: File, startTags: List[String], endTags: List[String], isXml: Boolean = true): RDD[String] = {
    val conf = new org.apache.hadoop.conf.Configuration()
    conf.setStrings(MultiLineTaggedInputFormat.START_TAG_KEY, startTags: _*)
    conf.setStrings(MultiLineTaggedInputFormat.END_TAG_KEY, endTags: _*)
    conf.setBoolean(MultiLineTaggedInputFormat.IS_XML_KEY, isXml)
    val rows = sparkContext.newAPIHadoopFile[LongWritable, Text, MultiLineTaggedInputFormat](file.getAbsolutePath,
      classOf[MultiLineTaggedInputFormat], classOf[LongWritable], classOf[Text], conf)
      .map(row => row._2.toString).filter(_.trim() != "")
    rows
  }
}
