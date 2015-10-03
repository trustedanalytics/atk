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
import org.trustedanalytics.atk.domain.frame.FrameReference
import org.trustedanalytics.atk.domain.model.ModelReference
import org.trustedanalytics.atk.domain.schema.{Column, DataTypes, FrameSchema}
import org.trustedanalytics.atk.testutils.TestingSparkContextWordSpec

class LdaCorpusTest extends TestingSparkContextWordSpec with Matchers {

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

  val model = new ModelReference(1)
  val frame = new FrameReference(1)
  val trainArgs = LdaTrainArgs(model, frame, "document", "word", "word_count")

  "LDA corpus" should {

    "add word Ids to edge frame" in {
      val rows = sparkContext.parallelize(edgeData)
      val edgeFrame = new FrameRdd(edgeSchema, rows)

      val ldaCorpus = LdaCorpus(edgeFrame, trainArgs)
      val edgesWithWordIds = ldaCorpus.addWordIdsToEdgeFrame().collect()
      val wordIdMap = ldaCorpus.uniqueWordsFrame.map(row => {
        (row(1).asInstanceOf[String], row(0).asInstanceOf[Long])
      }).collectAsMap()

      edgesWithWordIds.foreach(row => {
        val word = row(1).asInstanceOf[String]
        val wordId = row(3).asInstanceOf[Long]
        assert(wordId == wordIdMap(word))
      })
    }

    "create corpus of documents for training LDA model" in {
      val rows = sparkContext.parallelize(edgeData)
      val edgeFrame = new FrameRdd(edgeSchema, rows)

      val ldaCorpus = LdaCorpus(edgeFrame, trainArgs)
      val idWordMap = ldaCorpus.uniqueWordsFrame.map(row => {
        (row(0).asInstanceOf[Long], row(1).asInstanceOf[String])
      }).collectAsMap()

      val trainCorpus = ldaCorpus.createCorpus().collect()
      val docWordCountMap = edgeData.map(row => {
        val document = row(0).asInstanceOf[String]
        val word = row(1).asInstanceOf[String]
        val wordCount = row(2).asInstanceOf[Long]
        ((document, word), wordCount)
      }).toMap

      trainCorpus.foreach { case (docId, (doc, wordCountVector)) =>
        for (i <- wordCountVector.toArray.indices) {
          val wordCount = wordCountVector.toArray(i)
          val word = idWordMap(i.toLong)

          if (docWordCountMap.contains(doc, word)) {
            assert(wordCount == docWordCountMap(doc, word))
          }
          else {
            assert(wordCount == 0)
          }
        }
      }
    }

    "return empty frame" in {
      val rows = sparkContext.parallelize(Array.empty[Row])
      val edgeFrame = new FrameRdd(edgeSchema, rows)
      val ldaCorpus = LdaCorpus(edgeFrame, trainArgs)
      val trainCorpus = ldaCorpus.createCorpus().collect()

      assert(trainCorpus.isEmpty)
    }

    "throw an IllegalArgumentException if edge frame is null" in {
      intercept[IllegalArgumentException] {
        LdaCorpus(null, trainArgs)
      }
    }

    "throw an IllegalArgumentException if train arguments are null" in {
      intercept[IllegalArgumentException] {
        val rows = sparkContext.parallelize(Array.empty[Row])
        val edgeFrame = new FrameRdd(edgeSchema, rows)
        LdaCorpus(edgeFrame, null)
      }
    }

    "throw a SparkException for invalid column names" in {
      intercept[SparkException] {
        val rows = sparkContext.parallelize(edgeData)
        val edgeFrame = new FrameRdd(edgeSchema, rows)
        val invalidTrainArgs = LdaTrainArgs(model, frame, "invalid_document", "invalid_word", "invalid_count")
        LdaCorpus(edgeFrame, invalidTrainArgs).createCorpus()
      }
    }
  }
}
