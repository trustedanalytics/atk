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

package org.trustedanalytics.atk.engine.frame.plugins

import org.apache.spark.frame.FrameRdd
import org.apache.spark.sql.Row
import org.scalatest.{ FlatSpec, Matchers }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, FrameSchema }
import org.trustedanalytics.atk.testutils.TestingSparkContextFlatSpec

class DropDuplicatesByColumnITest extends FlatSpec with Matchers with TestingSparkContextFlatSpec {
  "dropDuplicatesByColumn" should "keep only unique rows for subset of columns" in {

    //setup test data
    val favoriteMovies = List(
      Row("John", 1, "Titanic"),
      Row("Kathy", 2, "Jurassic Park"),
      Row("John", 1, "The kite runner"),
      Row("Kathy", 2, "Toy Story 3"),
      Row("Peter", 3, "Star War"))

    val schema = FrameSchema(List(
      Column("name", DataTypes.string),
      Column("id", DataTypes.int32),
      Column("movie", DataTypes.string)))

    val rdd = sparkContext.parallelize(favoriteMovies)
    val frameRdd = new FrameRdd(schema, rdd)

    frameRdd.count() shouldBe 5

    //remove duplicates identified by column names
    val duplicatesRemoved = frameRdd.dropDuplicatesByColumn(List("name", "id")).collect()

    val expectedResults = Array(
      Row("John", 1, "Titanic"),
      Row("Kathy", 2, "Jurassic Park"),
      Row("Peter", 3, "Star War")
    )

    duplicatesRemoved should contain theSameElementsAs (expectedResults)
  }

  "dropDuplicatesByColumn" should "keep only unique rows" in {

    //setup test data
    val favoriteMovies = List(
      Row("John", 1, "Titanic"),
      Row("Kathy", 2, "Jurassic Park"),
      Row("Kathy", 2, "Jurassic Park"),
      Row("John", 1, "The kite runner"),
      Row("Kathy", 2, "Toy Story 3"),
      Row("Peter", 3, "Star War"),
      Row("Peter", 3, "Star War"))

    val schema = FrameSchema(List(
      Column("name", DataTypes.string),
      Column("id", DataTypes.int32),
      Column("movie", DataTypes.string)))

    val rdd = sparkContext.parallelize(favoriteMovies)
    val frameRdd = new FrameRdd(schema, rdd)

    frameRdd.count() shouldBe 7

    //remove duplicates identified by column names
    val duplicatesRemoved = frameRdd.dropDuplicatesByColumn(List("name", "id", "movie")).collect()

    val expectedResults = Array(
      Row("John", 1, "Titanic"),
      Row("John", 1, "The kite runner"),
      Row("Kathy", 2, "Jurassic Park"),
      Row("Kathy", 2, "Toy Story 3"),
      Row("Peter", 3, "Star War")
    )

    duplicatesRemoved should contain theSameElementsAs (expectedResults)
  }

  "dropDuplicatesByColumn" should "throw an IllegalArgumentException for invalid column names" in {
    intercept[IllegalArgumentException] {
      //setup test data
      val favoriteMovies = List(
        Row("John", 1, "Titanic"),
        Row("Kathy", 2, "Jurassic Park"),
        Row("John", 1, "The kite runner"),
        Row("Kathy", 2, "Toy Story 3"),
        Row("Peter", 3, "Star War"))

      val schema = FrameSchema(List(
        Column("name", DataTypes.string),
        Column("id", DataTypes.int32),
        Column("movie", DataTypes.string)))

      val rdd = sparkContext.parallelize(favoriteMovies)
      val frameRdd = new FrameRdd(schema, rdd)
      frameRdd.dropDuplicatesByColumn(List("name", "invalidCol1", "invalidCol2")).collect()
    }
  }
}
