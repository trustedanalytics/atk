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

package org.trustedanalytics.atk.domain

import org.scalatest.{ WordSpec, Matchers, FlatSpec }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.domain.schema._

class SchemaTest extends WordSpec with Matchers {

  val columns = List(Column("a", int64), Column("b", float32), Column("c", string))
  val abcSchema = new FrameSchema(columns)

  val ajColumns = List(Column("a", int64), Column("b", float32), Column("c", string),
    Column("d", string),
    Column("e", string),
    Column("f", string),
    Column("g", string),
    Column("h", string),
    Column("i", string),
    Column("j", string))
  val ajSchema = new FrameSchema(ajColumns)

  "FrameSchema" should {
    "find correct column index for single column" in {
      abcSchema.columnIndex("a") shouldBe 0
      abcSchema.columnIndex("b") shouldBe 1
      abcSchema.columnIndex("c") shouldBe 2
    }

    "find correct column index for two column" in {
      abcSchema.columnIndices(Seq("a", "b")) shouldBe List(0, 1)
      abcSchema.columnIndices(Seq("a", "c")) shouldBe List(0, 2)
    }

    "find correct column index for all columns when input columns is empty" in {
      abcSchema.columnIndices(Seq()) shouldBe List(0, 1, 2)
    }

    "be able to convert the type of a column from int64 to string" in {
      val result = abcSchema.convertType("a", string)
      assert(result.column("a").dataType == string)
      assert(result.column("b").dataType == float32)
      assert(result.column("c").dataType == string)
    }

    "be able to convert the type of a column from string to int" in {
      val result = abcSchema.convertType("b", int64)
      assert(result.column("a").dataType == int64)
      assert(result.column("b").dataType == int64)
      assert(result.column("c").dataType == string)
    }

    "be able to report column data types for first column" in {
      abcSchema.columnDataType("a") shouldBe int64
    }

    "be able to report column data types for last column" in {
      abcSchema.columnDataType("c") shouldBe string
    }

    "be able to require column types" in {
      val schema = FrameSchema(List(Column("a", DataTypes.vector(2)), Column("b", DataTypes.float64)))
      schema.requireColumnIsType("a", DataTypes.vector(2))
    }
    "be able to report bad vector type" in {
      val schema = FrameSchema(List(Column("a", DataTypes.vector(2)), Column("b", DataTypes.float64)))
      intercept[IllegalArgumentException] {
        schema.requireColumnIsType("a", DataTypes.float64)
      }
    }

    "be able to add columns" in {
      val added = abcSchema.addColumn("str", string)
      added.columns.length shouldBe 4
      added.column("str").dataType shouldBe string
    }

    "be able to add columns automatically fixing name conflicts" in {
      val added = abcSchema.addColumnsFixNames(Seq(Column("a", string)))
      added.columns.length shouldBe 4
      added.column("a").dataType shouldBe int64
      added.column("a_0").dataType shouldBe string
    }

    "be able to validate a column has a given type" in {
      abcSchema.hasColumnWithType("a", int64) shouldBe true
      abcSchema.hasColumnWithType("a", string) shouldBe false
    }

    def testDropColumn(columnName: String): Unit = {
      val result = abcSchema.dropColumn(columnName)
      assert(result.columns.length == 2, "length was not 2: " + result)
      assert(!result.hasColumn(columnName), "column was still present: " + result)
    }

    "be able to drop a column a" in {
      testDropColumn("a")
    }

    "be able to drop a column b" in {
      testDropColumn("b")
    }

    "be able to drop a column c" in {
      testDropColumn("c")
    }

    "be able to drop multiple columns 1" in {
      val result = abcSchema.dropColumns(List("a", "c"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("b"))
    }

    "be able to drop multiple columns 2" in {
      val result = abcSchema.dropColumns(List("a", "b"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("c"))
    }

    "be able to drop multiple columns 3" in {
      val result = abcSchema.dropColumns(List("b", "c"))
      assert(result.columns.length == 1)
      assert(result.hasColumn("a"))
    }

    "be able to drop multiple columns with list of 1" in {
      val result = abcSchema.dropColumns(List("a"))
      assert(result.columns.length == 2)
      assert(result.hasColumn("b"))
      assert(result.hasColumn("c"))
    }

    "be able to drop multiple columns by index 1" in {
      val result = abcSchema.dropColumnsByIndex(Seq(0, 2))
      assert(result.columns.length == 1)
      assert(result.hasColumn("b"))
    }

    "be able to drop multiple columns by index 2" in {
      val result = abcSchema.dropColumnsByIndex(Seq(0, 1))
      assert(result.columns.length == 1)
      assert(result.hasColumn("c"))
    }

    "be able to drop multiple columns by index 3" in {
      val result = abcSchema.dropColumnsByIndex(Seq(1, 2))
      assert(result.columns.length == 1)
      assert(result.hasColumn("a"))
    }

    "be able to drop multiple columns by index with list of 1" in {
      val result = abcSchema.dropColumnsByIndex(Seq(0))
      assert(result.columns.length == 2)
      assert(result.hasColumn("b"))
      assert(result.hasColumn("c"))
    }

    "be able to copy a subset of columns" in {
      abcSchema.copySubset(Seq("a", "c")).columnNames shouldBe List("a", "c")
    }

    "be able to rename columns" in {
      val renamed = abcSchema.renameColumn("b", "foo")
      renamed.hasColumn("b") shouldBe false
      renamed.hasColumn("foo") shouldBe true
      renamed.columnNames shouldBe List("a", "foo", "c")
    }

    "be able to reorder columns" in {
      val reordered = abcSchema.reorderColumns(List("b", "c", "a"))
      reordered.columnIndex("a") shouldBe 2
      reordered.columnIndex("b") shouldBe 0
      reordered.columnIndex("c") shouldBe 1
    }

    "be able to reorder columns with partial list" in {
      val reordered = abcSchema.reorderColumns(List("c", "a"))
      reordered.columnIndex("a") shouldBe 1
      reordered.columnIndex("b") shouldBe 2
      reordered.columnIndex("c") shouldBe 0
    }

    "return a subset of the columns as a List[Column]" in {
      val excluded = abcSchema.columnsExcept(List("a", "b"))
      excluded.length should be(1)
      excluded(0).name should be("c")
    }

    "be able to drop 'ignore' columns" in {
      val schema = new FrameSchema(List(Column("a", int64), Column("b", ignore), Column("c", string))).dropIgnoreColumns()
      assert(schema.columnTuples == List(("a", int64), ("c", string)))
    }

    "be able to select a subset and rename in one step" in {
      val schema = abcSchema.copySubsetWithRename(Map(("a", "a_renamed"), ("c", "c_renamed")))
      assert(schema.columns.length == 2)
      assert(schema.column(1).name == "c_renamed")
    }

    "be able to select a subset and rename to same names and preserve order" in {
      val schema = ajSchema.copySubsetWithRename(Map(("a", "a"), ("d", "d"), ("c", "c"), ("b", "b"), ("e", "e"), ("f", "f"), ("g", "g"), ("j", "j")))
      assert(schema.columns.length == 8)
      assert(schema.column(2).name == "c")
      assert(schema.column(6).name == "g")
      assert(schema.column(7).name == "j")
    }

    "be able to select a subset and rename to same names" in {
      val schema = abcSchema.copySubsetWithRename(Map(("a", "a"), ("c", "c")))
      assert(schema.columns.length == 2)
      assert(schema.column(1).name == "c")
    }

    "not allow renaming to duplicate column name" in {
      intercept[IllegalArgumentException] {
        abcSchema.renameColumn("c", "a")
      }
    }

    "optionally get a column when no column name is provided" in {
      assert(abcSchema.column(None) == None)
    }

    "optionally get a column when column name is provided" in {
      assert(abcSchema.column(Some("a")).get.name == "a")
    }

    "union schemas without overlapping columns" in {
      val result = abcSchema.union(vertexSchema)
      assert(result.columns.length == 7)
      assert(result.isInstanceOf[FrameSchema])
    }

    "union schemas with completely overlapping columns" in {
      val result = abcSchema.union(abcSchema)
      assert(result == abcSchema)
    }

    "union schemas with partially overlapping columns" in {
      val abSchema = abcSchema.copySubset(Seq("a", "b"))
      val bcSchema = abcSchema.copySubset(Seq("b", "c"))
      val result = abSchema.union(bcSchema)
      assert(result == abcSchema)
    }

    "not union schemas with columns of same name but different types" in {
      intercept[IllegalArgumentException] {
        val differentTypeSchema = abcSchema.copySubset(Seq("a", "b")).convertType("a", str)
        abcSchema.union(differentTypeSchema)
      }
    }

    "be able to validate column types when valid" in {
      abcSchema.requireColumnIsType("a", int64)
    }

    "be able to validate column types when invalid type" in {
      intercept[IllegalArgumentException] {
        abcSchema.requireColumnIsType("a", str)
      }
    }

    "be able to validate column types when invalid name" in {
      intercept[IllegalArgumentException] {
        abcSchema.requireColumnIsType("invalid", str)
      }
    }
    "be able to validate that column types are numeric" in {
      val numericColumns = List(Column("a", int64), Column("b", int32), Column("c", float32), Column("d", float64))
      val numericSchema = new FrameSchema(numericColumns)
      numericSchema.requireColumnsOfNumericPrimitives(List("a", "b", "c", "d"))
    }
    "be able to validate that column types are not numeric" in {
      intercept[IllegalArgumentException] {
        ajSchema.requireColumnsOfNumericPrimitives(List("d", "e", "f", "g"))
      }
    }

    "preserve column order in columnNames" in {
      ajSchema.columnNames shouldBe List("a", "b", "c", "d", "e", "f", "g", "h", "i", "j")
    }

    "preserve column order during union" in {
      abcSchema.union(ajSchema) shouldBe FrameSchema(ajColumns)
    }
  }

  "Schema" should {
    "resolve name conflicts when they exist" in {
      val leftColumns = List(Column("same", DataTypes.int32), Column("bar", DataTypes.int32))
      val rightColumns = List(Column("same", DataTypes.int32), Column("foo", DataTypes.string))

      val result = Schema.join(leftColumns, rightColumns)

      result.length shouldBe 4
      result(0).name shouldBe "same_L"
      result(1).name shouldBe "bar"
      result(2).name shouldBe "same_R"
      result(3).name shouldBe "foo"
    }

    "not do anything to resolve conflicts when they don't exist" in {
      val leftColumns = List(Column("left", DataTypes.int32), Column("bar", DataTypes.int32))
      val rightColumns = List(Column("right", DataTypes.int32), Column("foo", DataTypes.string))

      val result = Schema.join(leftColumns, rightColumns)

      result.length shouldBe 4
      result(0).name shouldBe "left"
      result(1).name shouldBe "bar"
      result(2).name shouldBe "right"
      result(3).name shouldBe "foo"
    }

    "handle empty column lists" in {
      val leftColumns = List(Column("left", DataTypes.int32), Column("bar", DataTypes.int32))
      val rightColumns = List.empty[Column]

      val result = Schema.join(leftColumns, rightColumns)

      result.length shouldBe 2
      result(0).name shouldBe "left"
      result(1).name shouldBe "bar"
    }

    "repeatedly appending L if L already exists in the left hand side" in {
      val leftColumns = List(Column("data", DataTypes.int32), Column("data_L", DataTypes.int32))
      val rightColumns = List(Column("data", DataTypes.int32))

      val result = Schema.join(leftColumns, rightColumns)
      result.length shouldBe 3
      result(0).name shouldBe "data_L_L"
      result(1).name shouldBe "data_L"
      result(2).name shouldBe "data_R"
    }

    "repeatedly appending L if L already exists in the right hand side" in {
      val leftColumns = List(Column("data", DataTypes.int32))
      val rightColumns = List(Column("data", DataTypes.int32), Column("data_L", DataTypes.int32))

      val result = Schema.join(leftColumns, rightColumns)
      result.length shouldBe 3
      result(0).name shouldBe "data_L_L"
      result(1).name shouldBe "data_R"
      result(2).name shouldBe "data_L"
    }

    "repeatedly appending R if R already exists in the left hand side" in {
      val leftColumns = List(Column("data", DataTypes.int32), Column("data_R", DataTypes.int32))
      val rightColumns = List(Column("data", DataTypes.int32))

      val result = Schema.join(leftColumns, rightColumns)
      result.length shouldBe 3
      result(0).name shouldBe "data_L"
      result(1).name shouldBe "data_R"
      result(2).name shouldBe "data_R_R"
    }

  }

  val vertexColumns = List(Column(GraphSchema.vidProperty, int64), Column(GraphSchema.labelProperty, str), Column("movie_id", int64), Column("name", str))
  val vertexSchema = VertexSchema(vertexColumns, "movies", Some("movie_id"))

  "VertexSchema" should {

    "find correct column index for single column" in {
      vertexSchema.columnIndex(GraphSchema.vidProperty) shouldBe 0
      vertexSchema.columnIndex(GraphSchema.labelProperty) shouldBe 1
      vertexSchema.columnIndex("movie_id") shouldBe 2
    }

    "find correct column index for two column" in {
      vertexSchema.columnIndices(Seq(GraphSchema.vidProperty, GraphSchema.labelProperty)) shouldBe List(0, 1)
      vertexSchema.columnIndices(Seq(GraphSchema.vidProperty, "movie_id")) shouldBe List(0, 2)
    }

    "find correct column index for all columns when input columns is empty" in {
      vertexSchema.columnIndices(Seq()) shouldBe List(0, 1, 2, 3)
    }

    "be able to report column data types for first column" in {
      vertexSchema.columnDataType(GraphSchema.vidProperty) shouldBe int64
    }

    "be able to report column data types for last column" in {
      vertexSchema.columnDataType("name") shouldBe string
    }

    "be able to add columns" in {
      val added = vertexSchema.addColumn("str", string)
      added.columns.length shouldBe 5
      added.column("str").dataType shouldBe string
      assert(added.isInstanceOf[VertexSchema])
    }

    "be able to validate a column has a given type" in {
      vertexSchema.hasColumnWithType(GraphSchema.vidProperty, int64) shouldBe true
      vertexSchema.hasColumnWithType(GraphSchema.vidProperty, string) shouldBe false

    }

    def testDropColumn(columnName: String): Unit = {
      val result = vertexSchema.dropColumn(columnName)
      assert(result.columns.length == 2, "length was not 2: " + result)
      assert(!result.hasColumn(columnName), "column was still present: " + result)
      assert(result.isInstanceOf[VertexSchema])
    }

    "not be able to drop _vid column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.vidProperty)
      }
    }

    "not be able to drop _label column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.labelProperty)
      }
    }

    "not be able to drop movie_id column" in {
      intercept[IllegalArgumentException] {
        testDropColumn("movie_id")
      }
    }

    "be able to drop multiple columns" in {
      val result = vertexSchema.dropColumns(List("name"))
      assert(result.columns.length == 3)
      assert(result.hasColumn(GraphSchema.vidProperty))
      assert(result.hasColumn(GraphSchema.labelProperty))
      assert(result.hasColumn("movie_id"))
      assert(result.isInstanceOf[VertexSchema])
      assert(result.isInstanceOf[VertexSchema])
    }

    "be able to copy a subset of columns" in {
      vertexSchema.copySubset(Seq(GraphSchema.vidProperty, GraphSchema.labelProperty, "movie_id")).columns.length shouldBe 3
    }

    "be able to rename columns" in {
      val renamed = vertexSchema.renameColumn("name", "title")
      renamed.hasColumn("name") shouldBe false
      renamed.hasColumn("title") shouldBe true
      renamed.columnNames shouldBe List(GraphSchema.vidProperty, GraphSchema.labelProperty, "movie_id", "title")
      assert(renamed.isInstanceOf[VertexSchema])
    }

    "be able to rename movie_id column" in {
      val renamed = vertexSchema.renameColumn("movie_id", "m_id")
      renamed.hasColumn("movie_id") shouldBe false
      renamed.hasColumn("m_id") shouldBe true
      renamed.asInstanceOf[VertexSchema].idColumnName.get shouldBe "m_id"
    }

    "be able to rename a subset of columns" in {
      val renamed = vertexSchema.renameColumns(Map("movie_id" -> "m_id", "name" -> "title"))
      renamed.hasColumn("movie_id") shouldBe false
      renamed.hasColumn("m_id") shouldBe true
      renamed.hasColumn("name") shouldBe false
      renamed.hasColumn("title") shouldBe true
      renamed.columnNames shouldBe List(GraphSchema.vidProperty, GraphSchema.labelProperty, "m_id", "title")
      renamed.asInstanceOf[VertexSchema].idColumnName.get shouldBe "m_id"
    }

    "not be able to rename _vid column" in {
      intercept[IllegalArgumentException] {
        vertexSchema.renameColumn(GraphSchema.vidProperty, "other")
      }
    }

    "not be able to rename non-existent columns" in {
      intercept[IllegalArgumentException] {
        vertexSchema.renameColumn("invalid_col", "other")
      }
    }

    "not be able to rename _label column" in {
      intercept[IllegalArgumentException] {
        vertexSchema.renameColumn(GraphSchema.labelProperty, "other")
      }
    }

    "return a subset of the columns as a List[Column]" in {
      val excluded = vertexSchema.columnsExcept(List("name"))
      excluded.length should be(3)
    }

    "be able to select a subset and rename" in {
      val schema = vertexSchema.copySubsetWithRename(Map((GraphSchema.vidProperty, GraphSchema.vidProperty), (GraphSchema.labelProperty, GraphSchema.labelProperty), ("movie_id", "m_id")))
      assert(schema.columns.length == 3)
      assert(schema.column(2).name == "m_id")
      assert(schema.isInstanceOf[VertexSchema])
    }

    "be convertible to a FrameSchema" in {
      val schema = vertexSchema.toFrameSchema
      assert(schema.isInstanceOf[FrameSchema])
    }

    "require a _vid and _label column" in {
      intercept[IllegalArgumentException] {
        new VertexSchema(columns, "movies", None)
      }
    }

    "require a _vid column" in {
      intercept[IllegalArgumentException] {
        new VertexSchema(List(Column(GraphSchema.labelProperty, str)), "movies", None)
      }
    }

    "require a _label column" in {
      intercept[IllegalArgumentException] {
        new VertexSchema(List(Column(GraphSchema.vidProperty, int64)), "movies", None)
      }
    }

    "require a _vid column to be int64" in {
      intercept[IllegalArgumentException] {
        new VertexSchema(List(Column(GraphSchema.vidProperty, str), Column(GraphSchema.labelProperty, str)), "movies", None)
      }
    }

    "determine id column name when already defined" in {
      assert(vertexSchema.determineIdColumnName("other_name") == "movie_id")
    }

    "determine id column name when not already defined" in {
      val v2 = new VertexSchema(List(Column(GraphSchema.vidProperty, int64), Column(GraphSchema.labelProperty, str)), "movies", None)
      assert(v2.determineIdColumnName("other_name") == "other_name")
    }

    "be able to reassign idColumnName in copy" in {
      val v1 = new VertexSchema(List(Column(GraphSchema.vidProperty, int64), Column(GraphSchema.labelProperty, str), Column("movie_id", int64)), "movies", None)
      val v2 = v1.copy(idColumnName = Some("movie_id"))
      assert(v1.columns == v2.columns)
      assert(v2.idColumnName == Some("movie_id"))
    }
  }

  val edgeColumns = List(Column(GraphSchema.edgeProperty, int64), Column(GraphSchema.labelProperty, str), Column(GraphSchema.srcVidProperty, int64), Column(GraphSchema.destVidProperty, int64), Column("rating", str))
  val edgeSchema = EdgeSchema(edgeColumns, "ratings", "users", "movies", directed = true)

  "EdgeSchema" should {

    "find correct column index for single column" in {
      edgeSchema.columnIndex(GraphSchema.edgeProperty) shouldBe 0
      edgeSchema.columnIndex(GraphSchema.labelProperty) shouldBe 1
      edgeSchema.columnIndex("rating") shouldBe 4
    }

    "find correct column index for two column" in {
      edgeSchema.columnIndices(Seq(GraphSchema.edgeProperty, GraphSchema.labelProperty)) shouldBe List(0, 1)
      edgeSchema.columnIndices(Seq(GraphSchema.edgeProperty, "rating")) shouldBe List(0, 4)
    }

    "find correct column index for all columns when input columns is empty" in {
      edgeSchema.columnIndices(Seq()) shouldBe List(0, 1, 2, 3, 4)
    }

    "be able to report column data types for first column" in {
      edgeSchema.columnDataType(GraphSchema.edgeProperty) shouldBe int64
    }

    "be able to report column data types for last column" in {
      edgeSchema.columnDataType("rating") shouldBe string
    }

    "be able to add columns" in {
      val added = edgeSchema.addColumn("str", string)
      added.columns.length shouldBe 6
      added.column("str").dataType shouldBe string
    }

    "be able to validate a column has a given type" in {
      edgeSchema.hasColumnWithType(GraphSchema.edgeProperty, int64) shouldBe true
      edgeSchema.hasColumnWithType(GraphSchema.edgeProperty, string) shouldBe false

    }

    def testDropColumn(columnName: String): Unit = {
      val result = edgeSchema.dropColumn(columnName)
      assert(result.columns.length == 2, "length was not 2: " + result)
      assert(!result.hasColumn(columnName), "column was still present: " + result)
    }

    "not be able to drop _eid column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.edgeProperty)
      }
    }

    "not be able to drop _label column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.labelProperty)
      }
    }

    "not be able to drop _src_vid column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.srcVidProperty)
      }
    }

    "not be able to drop _dest_vid column" in {
      intercept[IllegalArgumentException] {
        testDropColumn(GraphSchema.destVidProperty)
      }
    }

    "be able to drop multiple columns" in {
      val result = edgeSchema.dropColumns(List("rating"))
      assert(result.columns.length == 4)
      assert(result.hasColumn(GraphSchema.edgeProperty))
      assert(result.hasColumn(GraphSchema.labelProperty))
    }

    "be able to rename columns" in {
      val renamed = edgeSchema.renameColumn("rating", "user_rating")
      renamed.hasColumn("rating") shouldBe false
      renamed.hasColumn("user_rating") shouldBe true
    }

    "not be able to rename _eid column" in {
      intercept[IllegalArgumentException] {
        edgeSchema.renameColumn(GraphSchema.edgeProperty, "other")
      }
    }

    "not be able to rename _label column" in {
      intercept[IllegalArgumentException] {
        edgeSchema.renameColumn(GraphSchema.labelProperty, "other")
      }
    }

    "return a subset of the columns as a List[Column]" in {
      val excluded = edgeSchema.columnsExcept(List("rating"))
      excluded.length should be(4)
    }

    "be able to select a subset and rename" in {
      val schema = edgeSchema.copySubsetWithRename(Map((GraphSchema.edgeProperty, GraphSchema.edgeProperty),
        (GraphSchema.labelProperty, GraphSchema.labelProperty),
        (GraphSchema.srcVidProperty, GraphSchema.srcVidProperty),
        (GraphSchema.destVidProperty, GraphSchema.destVidProperty),
        ("rating", "stars")))
      assert(schema.columns.length == 5)
      assert(schema.hasColumn(GraphSchema.edgeProperty))
      assert(schema.hasColumn(GraphSchema.labelProperty))
      assert(schema.hasColumn("stars"))
      assert(!schema.hasColumn("rating"))
    }

    "be convertible to a FrameSchema" in {
      val schema = edgeSchema.toFrameSchema
      assert(schema.isInstanceOf[FrameSchema])
    }

    "require a _eid column" in {
      intercept[IllegalArgumentException] {
        new EdgeSchema(List(Column(GraphSchema.labelProperty, str), Column(GraphSchema.srcVidProperty, int64), Column(GraphSchema.destVidProperty, int64), Column("rating", str)), "ratings", "users", "movies", directed = true)
      }
    }

    "require a _label column" in {
      intercept[IllegalArgumentException] {
        new EdgeSchema(List(Column(GraphSchema.edgeProperty, int64), Column(GraphSchema.srcVidProperty, int64), Column(GraphSchema.destVidProperty, int64), Column("rating", str)), "ratings", "users", "movies", directed = true)
      }
    }

    "require a _src_vid column" in {
      intercept[IllegalArgumentException] {
        new EdgeSchema(List(Column(GraphSchema.edgeProperty, int64), Column(GraphSchema.labelProperty, str), Column(GraphSchema.destVidProperty, int64), Column("rating", str)), "ratings", "users", "movies", directed = true)
      }
    }

    "require a _dest_vid column" in {
      intercept[IllegalArgumentException] {
        new EdgeSchema(List(Column(GraphSchema.edgeProperty, int64), Column(GraphSchema.labelProperty, str), Column(GraphSchema.srcVidProperty, int64), Column("rating", str)), "ratings", "users", "movies", directed = true)
      }
    }

    "require a _eid column to be int64" in {
      intercept[IllegalArgumentException] {
        new EdgeSchema(List(Column(GraphSchema.edgeProperty, str), Column(GraphSchema.labelProperty, str), Column(GraphSchema.srcVidProperty, int64), Column(GraphSchema.destVidProperty, int64), Column("rating", str)), "ratings", "users", "movies", directed = true)
      }
    }

  }

}
