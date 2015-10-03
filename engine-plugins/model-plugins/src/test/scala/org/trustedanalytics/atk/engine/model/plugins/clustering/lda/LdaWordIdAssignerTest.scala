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
package org.trustedanalytics.atk.engine.model.plugins.clustering.lda

import org.apache.spark.SparkException
import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.Matchers
import org.trustedanalytics.atk.domain.schema.{DataTypes, Column, FrameSchema}
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class LdaWordIdAssignerTest extends TestingSparkContextWordSpec with Matchers {
  val edgeData: Array[Row] = Array(
    Row("nytimes", "harry", 3L),
    Row("nytimes", "economy", 35L),
    Row("nytimes", "jobs", 40L),
    Row("nytimes", "magic", 1L),
    Row("nytimes", "realestate", 15L),
    Row("nytimes", "movies", 6L),
    Row("economist", "economy", 50L),
    Row("economist", "jobs", 35L),
    Row("economist", "realestate", 20L),
    Row("economist", "movies", 1L),
    Row("economist", "harry", 1L),
    Row("economist", "magic", 1L),
    Row("harrypotter", "harry", 40L),
    Row("harrypotter", "magic", 30L),
    Row("harrypotter", "chamber", 20L),
    Row("harrypotter", "secrets", 30L)
  )

  val edgeSchema = FrameSchema(List(
    Column("document", DataTypes.string),
    Column("word", DataTypes.string),
    Column("word_count", DataTypes.int64)
  ))

  "LDA word Id assigner" should {

    "return frame with unique word ids, words, and counts" in {
      val rows = sparkContext.parallelize(edgeData)
      val edgeFrame = new FrameRdd(edgeSchema, rows)
      val uniqueWords = LdaWordIdAssigner(edgeFrame, "word", "word_count").assignUniqueIds().collect()

      val wordIds = uniqueWords.map(row => row(0).asInstanceOf[Long])
      val wordCounts = uniqueWords.map(row => (row(1).asInstanceOf[String], row(2).asInstanceOf[Long])).toMap

      wordIds.sorted should contain theSameElementsInOrderAs Array(0, 1, 2, 3, 4, 5, 6, 7)
      assert(wordCounts("harry") == 44)
      assert(wordCounts("economy") == 85)
      assert(wordCounts("jobs") == 75)
      assert(wordCounts("magic") == 32)
      assert(wordCounts("realestate") == 35)
      assert(wordCounts("movies") == 7)
      assert(wordCounts("chamber") == 20)
      assert(wordCounts("secrets") == 30)
    }

    "return empty frame" in {
      val rows = sparkContext.parallelize(Array.empty[Row])
      val edgeFrame = new FrameRdd(edgeSchema, rows)
      val uniqueWords =  LdaWordIdAssigner(edgeFrame, "word", "word_count").assignUniqueIds().collect()

      assert (uniqueWords.isEmpty)
    }

    "throw a SparkException for invalid column names" in {
      intercept[SparkException]{
        val rows = sparkContext.parallelize(edgeData)
        val edgeFrame = new FrameRdd(edgeSchema, rows)
        LdaWordIdAssigner(edgeFrame, "invalid_word", "invalid_word_count").assignUniqueIds().collect()
      }
    }
  }
}
