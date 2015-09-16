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

package org.trustedanalytics.atk.domain.schema

import org.trustedanalytics.atk.StringUtils
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType

/**
 * Column - this is a nicer wrapper for columns than just tuples
 *
 * @param name the column name
 * @param dataType the type
 * @param index Columns can track their own indices once they are added to a schema, -1 if not defined
 *              This field should go away - it is bad because Schemas and Columns are otherwise immutable,
 *              so this can result in some surprising bugs.
 */
case class Column(name: String, dataType: DataType,
                  @deprecated("we should remove this field, please don't use") var index: Int = -1) {
  require(name != null, "column name is required")
  require(dataType != null, "column data type is required")
  require(name != "", "column name can't be empty")
  require(StringUtils.isAlphanumericUnderscore(name), "column name must be alpha-numeric with underscores")
}

/**
 * Extra schema if this is a vertex frame
 */
case class VertexSchema(columns: List[Column] = List[Column](), label: String, idColumnName: Option[String] = None) extends GraphElementSchema {
  require(hasColumnWithType(GraphSchema.vidProperty, DataTypes.int64), "schema did not have int64 _vid column: " + columns)
  require(hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have string _label column: " + columns)
  if (idColumnName != null) {
    //require(hasColumn(vertexSchema.get.idColumnName), s"schema must contain vertex id column ${vertexSchema.get.idColumnName}")
  }

  /**
   * If the id column name had already been defined, use that name, otherwise use the supplied name
   * @param nameIfNotAlreadyDefined name to use if not defined
   * @return the name to use
   */
  def determineIdColumnName(nameIfNotAlreadyDefined: String): String = {
    idColumnName.getOrElse(nameIfNotAlreadyDefined)
  }

  /**
   * Rename a column
   * @param existingName the old name
   * @param newName the new name
   * @return the updated schema
   */
  override def renameColumn(existingName: String, newName: String): Schema = {
    renameColumns(Map(existingName -> newName))
  }

  /**
   * Renames several columns
   * @param names oldName -> newName
   * @return new renamed schema
   */
  override def renameColumns(names: Map[String, String]): Schema = {
    val newSchema = super.renameColumns(names)
    val newIdColumn = idColumnName match {
      case Some(id) => Some(names.getOrElse(id, id))
      case _ => idColumnName
    }
    new VertexSchema(newSchema.columns, label, newIdColumn)
  }

  override def copy(columns: List[Column]): VertexSchema = {
    new VertexSchema(columns, label, idColumnName)
  }

  def copy(idColumnName: Option[String]): VertexSchema = {
    new VertexSchema(columns, label, idColumnName)
  }

  override def dropColumn(columnName: String): Schema = {
    if (idColumnName.isDefined) {
      require(idColumnName.get != columnName, s"The id column is not allowed to be dropped: $columnName")
    }

    if (GraphSchema.vertexSystemColumnNames.contains(columnName)) {
      throw new IllegalArgumentException(s"$columnName is a system column that is not allowed to be dropped")
    }

    super.dropColumn(columnName)
  }

}

/**
 * Extra schema if this is an edge frame
 * @param label the label for this edge list
 * @param srcVertexLabel the src "type" of vertices this edge connects
 * @param destVertexLabel the destination "type" of vertices this edge connects
 * @param directed true if edges are directed, false if they are undirected
 */
case class EdgeSchema(columns: List[Column] = List[Column](), label: String, srcVertexLabel: String, destVertexLabel: String, directed: Boolean = false) extends GraphElementSchema {
  require(hasColumnWithType(GraphSchema.edgeProperty, DataTypes.int64), "schema did not have int64 _eid column: " + columns)
  require(hasColumnWithType(GraphSchema.srcVidProperty, DataTypes.int64), "schema did not have int64 _src_vid column: " + columns)
  require(hasColumnWithType(GraphSchema.destVidProperty, DataTypes.int64), "schema did not have int64 _dest_vid column: " + columns)
  require(hasColumnWithType(GraphSchema.labelProperty, DataTypes.str), "schema did not have string _label column: " + columns)

  override def copy(columns: List[Column]): EdgeSchema = {
    new EdgeSchema(columns, label, srcVertexLabel, destVertexLabel, directed)
  }

  override def dropColumn(columnName: String): Schema = {

    if (GraphSchema.edgeSystemColumnNamesSet.contains(columnName)) {
      throw new IllegalArgumentException(s"$columnName is a system column that is not allowed to be dropped")
    }

    super.dropColumn(columnName)
  }
}

/**
 * Schema for a data frame. Contains the columns with names and data types.
 * @param columns the columns in the data frame
 */
case class FrameSchema(columns: List[Column] = List[Column]()) extends Schema {

  override def copy(columns: List[Column]): FrameSchema = {
    new FrameSchema(columns)
  }

}

/**
 * Common interface for Vertices and Edges
 */
trait GraphElementSchema extends Schema {

  /** Vertex or Edge label */
  def label: String

}

object Schema {

  /**
   * Join two lists of columns.
   *
   * Join resolves naming conflicts when both left and right columns have same column names
   *
   * @param leftColumns columns for the left side
   * @param rightColumns columns for the right side
   * @return Combined list of columns
   */
  def join(leftColumns: List[Column], rightColumns: List[Column]): List[Column] = {

    val funcAppendLetterForConflictingNames = (left: List[Column], right: List[Column], appendLetter: String) => {

      var leftColumnNames = left.map(column => column.name)
      val rightColumnNames = right.map(column => column.name)

      left.map(column =>
        if (right.map(i => i.name).contains(column.name)) {
          var name = column.name + "_" + appendLetter
          while (leftColumnNames.contains(name) || rightColumnNames.contains(name)) {
            name = name + "_" + appendLetter
          }
          leftColumnNames = leftColumnNames ++ List(name)
          Column(name, column.dataType)
        }
        else
          column)
    }

    val left = funcAppendLetterForConflictingNames(leftColumns, rightColumns, "L")
    val right = funcAppendLetterForConflictingNames(rightColumns, leftColumns, "R")

    left ++ right
  }
}

/**
 * Schema for a data frame. Contains the columns with names and data types.
 */
trait Schema {

  val columns: List[Column]

  require(columns != null, "columns must not be null")
  require({
    val distinct = columns.map(_.name).distinct
    distinct.length == columns.length
  }, s"invalid schema, column names cannot be duplicated: $columns")

  // assign indices
  columns.zipWithIndex.foreach { case (column, index) => column.index = index }

  /**
   * Map of names to columns
   */
  private lazy val namesToColumns = columns.map(col => (col.name, col)).toMap

  def copy(columns: List[Column]): Schema

  def columnNames: List[String] = {
    columns.map(col => col.name)
  }

  /**
   * True if this schema contains the supplied columnName
   */
  def hasColumn(columnName: String): Boolean = {
    namesToColumns.contains(columnName)
  }

  /**
   * True if this schema contains all of the supplied columnNames
   */
  def hasColumns(columnNames: Seq[String]): Boolean = {
    columnNames.forall(hasColumn)
  }

  /**
   * True if this schema contains the supplied columnName with the given dataType
   */
  def hasColumnWithType(columnName: String, dataType: DataType): Boolean = {
    hasColumn(columnName) && column(columnName).dataType.equalsDataType(dataType)
  }

  /**
   * Validate that the list of column names provided exist in this schema
   * throwing an exception if any does not exist.
   */
  def validateColumnsExist(columnNames: Iterable[String]): Iterable[String] = {
    columnIndices(columnNames.toSeq)
    columnNames
  }

  /**
   * Validate that all columns are of numeric data type
   */
  def requireColumnsOfNumericPrimitives(columnNames: Iterable[String]) = {
    columnNames.foreach(columnName => {
      require(hasColumn(columnName), s"column $columnName was not found")
      require(columnDataType(columnName).isNumerical, s"column $columnName should be of type numeric")
    })
  }

  /**
   * Validate that a column exists, and has the expected data type
   */
  def requireColumnIsType(columnName: String, dataType: DataType): Unit = {
    require(hasColumn(columnName), s"column $columnName was not found")
    require(columnDataType(columnName).equalsDataType(dataType), s"column $columnName should be of type $dataType")
  }

  /**
   * Validate that a column exists, and has the expected data type by supplying a custom checker, like isVectorDataType
   */
  def requireColumnIsType(columnName: String, dataTypeChecker: DataType => Boolean): Unit = {
    require(hasColumn(columnName), s"column $columnName was not found")
    val colDataType = columnDataType(columnName)
    require(dataTypeChecker(colDataType), s"column $columnName has bad type $colDataType")
  }

  def requireColumnIsNumerical(columnName: String): Unit = {
    val colDataType = columnDataType(columnName)
    require(colDataType.isNumerical, s"Column $columnName was not numerical. Expected a numerical data type, but got $colDataType.")
  }

  /**
   * Either single column name that is a vector or a list of columns that are numeric primitives
   * that can be converted to a vector.
   *
   * List cannot be empty
   */
  def requireColumnsAreVectorizable(columnNames: List[String]): Unit = {
    require(columnNames.nonEmpty, "single vector column, or one or more numeric columns required")
    if (columnNames.size > 1) {
      requireColumnsOfNumericPrimitives(columnNames)
    }
    else {
      val colDataType = columnDataType(columnNames.head)
      require(colDataType.isVector || colDataType.isNumerical, s"column ${columnNames.head} should be of type numeric")
    }
  }

  /**
   * Column names as comma separated list in a single string
   * (useful for error messages, etc)
   */
  def columnNamesAsString: String = {
    columnNames.mkString(", ")
  }

  // TODO: add a rename column method, since renaming columns shows up in Edge and Vertex schema it is more complicated

  /**
   * get column index by column name
   *
   * Throws exception if not found, check first with hasColumn()
   *
   * @param columnName name of the column to find index
   */
  def columnIndex(columnName: String): Int = {
    val index = columns.indexWhere(column => column.name == columnName, 0)
    if (index == -1)
      throw new IllegalArgumentException(s"Invalid column name $columnName provided, please choose from: " + columnNamesAsString)
    else
      index
  }

  /**
   * Retrieve list of column index based on column names
   * @param columnNames input column names
   */
  def columnIndices(columnNames: Seq[String]): Seq[Int] = {
    if (columnNames.isEmpty)
      columns.indices.toList
    else {
      columnNames.map(columnName => columnIndex(columnName))
    }
  }

  /**
   * Copy a subset of columns into a new Schema
   * @param columnNames the columns to keep
   * @return the new Schema
   */
  def copySubset(columnNames: Seq[String]): Schema = {
    val indices = columnIndices(columnNames)
    val columnSubset = indices.map(i => columns(i)).toList
    copy(columnSubset)
  }

  /**
   * Produces a renamed subset schema from this schema
   * @param columnNamesWithRename rename mapping
   * @return new schema
   */
  def copySubsetWithRename(columnNamesWithRename: Map[String, String]): Schema = {
    val preservedOrderColumnNames = columnNames.filter(name => columnNamesWithRename.contains(name))
    copySubset(preservedOrderColumnNames).renameColumns(columnNamesWithRename)
  }

  /**
   * Union schemas together, keeping as much info as possible.
   *
   * Vertex and/or Edge schema information will be maintained for this schema only
   *
   * Column type conflicts will cause error
   */
  def union(schema: Schema): Schema = {
    // check for conflicts
    val newColumns: List[Column] = schema.columns.filterNot(c => {
      hasColumn(c.name) && {
        require(hasColumnWithType(c.name, c.dataType), s"columns with same name ${c.name} didn't have matching types"); true
      }
    })
    val combinedColumns = this.columns ++ newColumns
    copy(combinedColumns)
  }

  /**
   * get column datatype by column name
   * @param columnName name of the column
   */
  def columnDataType(columnName: String): DataType = {
    column(columnName).dataType
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnName the name of the column
   * @return complete column info
   */
  def column(columnName: String): Column = {
    namesToColumns.getOrElse(columnName, throw new IllegalArgumentException(s"No column named $columnName choose between: $columnNamesAsString"))
  }

  /**
   * Convenience method for optionally getting a Column.
   *
   * This is helpful when specifying a columnName was optional for the user.
   *
   * @param columnName the name of the column
   * @return complete column info, if a name was provided
   */
  def column(columnName: Option[String]): Option[Column] = {
    columnName match {
      case Some(name) => Some(column(name))
      case None => None
    }
  }

  /**
   * Select a subset of columns.
   *
   * List can be empty.
   */
  def columns(columnNames: List[String]): List[Column] = {
    columnNames.map(column)
  }

  /**
   * Validates a Map argument used for renaming schema, throwing exceptions for violations
   *
   * @param names victimName -> newName
   */
  def validateRenameMapping(names: Map[String, String], forCopy: Boolean = false): Unit = {
    if (names.isEmpty)
      throw new IllegalArgumentException(s"Empty column name map provided.  At least one name is required")
    val victimNames = names.keys.toList
    validateColumnsExist(victimNames)
    val newNames = names.values.toList
    if (newNames.size != newNames.distinct.size) {
      throw new IllegalArgumentException(s"Invalid new column names are not unique: $newNames")
    }
    if (!forCopy) {
      val safeNames = columnNamesExcept(victimNames)
      for (n <- newNames) {
        if (safeNames.contains(n)) {
          throw new IllegalArgumentException(s"Invalid new column name '$n' collides with existing names which are not being renamed: $safeNames")
        }
      }
    }
  }

  /**
   * Get all of the info about a column - this is a nicer wrapper than tuples
   *
   * @param columnIndex the index for the column
   * @return complete column info
   */
  def column(columnIndex: Int): Column = columns(columnIndex)

  /**
   * Add a column to the schema
   * @param columnName name
   * @param dataType the type for the column
   * @return a new copy of the Schema with the column added
   */
  def addColumn(columnName: String, dataType: DataType): Schema = {
    if (columnNames.contains(columnName)) {
      throw new IllegalArgumentException(s"Cannot add a duplicate column name: $columnName")
    }
    copy(columns = columns :+ Column(columnName, dataType))
  }

  /**
   * Add a column if it doesn't already exist.
   *
   * Throws error if column name exists with different data type
   */
  def addColumnIfNotExists(columnName: String, dataType: DataType): Schema = {
    if (hasColumn(columnName)) {
      requireColumnIsType(columnName, DataTypes.float64)
      this
    }
    else {
      addColumn(columnName, dataType)
    }
  }

  /**
   * Returns a new schema with the given columns appended.
   */
  def addColumns(newColumns: Seq[Column]): Schema = {
    copy(columns = columns ++ newColumns)
  }

  /**
   * Add column but fix the name automatically if there is any conflicts with existing column names.
   */
  def addColumnFixName(column: Column): Schema = {
    this.addColumn(this.getNewColumnName(column.name), column.dataType)
  }

  /**
   * Add columns but fix the names automatically if there are any conflicts with existing column names.
   */
  def addColumnsFixNames(columnsToAdd: Seq[Column]): Schema = {
    var schema = this
    for {
      column <- columnsToAdd
    } {
      schema = schema.addColumnFixName(column)
    }
    schema
  }

  /**
   * Remove a column from this schema
   * @param columnName the name to remove
   * @return a new copy of the Schema with the column removed
   */
  def dropColumn(columnName: String): Schema = {
    copy(columns = columns.filterNot(column => column.name == columnName))
  }

  /**
   * Remove a list of columns from this schema
   * @param columnNames the names to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumns(columnNames: List[String]): Schema = {
    var newSchema = this
    if (columnNames != null) {
      columnNames.foreach(columnName => {
        newSchema = newSchema.dropColumn(columnName)
      })
    }
    newSchema
  }

  /**
   * Drop all columns with the 'ignore' data type.
   *
   * The ignore data type is a slight hack for ignoring some columns on import.
   */
  def dropIgnoreColumns(): Schema = {
    dropColumns(columns.filter(col => col.dataType.equalsDataType(DataTypes.ignore)).map(col => col.name))
  }

  /**
   * Remove columns by the indices
   * @param columnIndices the indices to remove
   * @return a new copy of the Schema with the columns removed
   */
  def dropColumnsByIndex(columnIndices: Seq[Int]): Schema = {
    val remainingColumns = columns.zipWithIndex.filterNot {
      case (col, index) =>
        columnIndices.contains(index)
    }.map { case (col, index) => col }

    copy(remainingColumns)
  }

  /**
   * Convert data type for a column
   * @param columnName the column to change
   * @param updatedDataType the new data type for that column
   * @return the updated Schema
   */
  def convertType(columnName: String, updatedDataType: DataType): Schema = {
    val col = column(columnName)
    val index = columnIndex(columnName)
    copy(columns = columns.updated(index, col.copy(dataType = updatedDataType)))
  }

  /**
   * Rename a column
   * @param existingName the old name
   * @param newName the new name
   * @return the updated schema
   */
  def renameColumn(existingName: String, newName: String): Schema = {
    renameColumns(Map(existingName -> newName))
  }

  /**
   * Renames several columns
   * @param names oldName -> newName
   * @return new renamed schema
   */
  def renameColumns(names: Map[String, String]): Schema = {
    validateRenameMapping(names)
    copy(columns = columns.map({
      case found if names.contains(found.name) => found.copy(name = names(found.name))
      case notFound => notFound.copy()
    }))
  }

  /**
   * Re-order the columns in the schema.
   *
   * No columns will be dropped.  Any column not named will be tacked onto the end.
   *
   * @param columnNames the names you want to occur first, in the order you want
   * @return the updated schema
   */
  def reorderColumns(columnNames: List[String]): Schema = {
    validateColumnsExist(columnNames)
    val reorderedColumns = columnNames.map(name => column(name))
    val additionalColumns = columns.filterNot(column => columnNames.contains(column.name))
    copy(columns = reorderedColumns ++ additionalColumns)
  }

  /**
   * Get the list of columns except those provided
   * @param columnNamesToExclude columns you want to filter
   * @return the other columns, if any
   */
  def columnsExcept(columnNamesToExclude: List[String]): List[Column] = {
    this.columns.filter(column => !columnNamesToExclude.contains(column.name))
  }

  /**
   * Get the list of column names except those provided
   * @param columnNamesToExclude column names you want to filter
   * @return the other column names, if any
   */
  def columnNamesExcept(columnNamesToExclude: List[String]): List[String] = {
    for { c <- columns if !columnNamesToExclude.contains(c.name) } yield c.name
  }

  /**
   * Legacy column format for schemas
   *
   * Schema was defined previously as a list of tuples.  This method was introduced to so
   * all of the dependent code wouldn't need to be changed.
   */
  @deprecated("legacy use only - use nicer API instead")
  def columnTuples: List[(String, DataType)] = {
    columns.map(column => (column.name, column.dataType))
  }

  /**
   * Legacy copy() method
   *
   * Schema was defined previously as a list of tuples.  This method was introduced to so
   * all of the dependent code wouldn't need to be changed.
   */
  @deprecated("don't use - legacy support only")
  def legacyCopy(columnTuples: List[(String, DataType)]): Schema = {
    val updated = columnTuples.map { case (name, dataType) => Column(name, dataType) }
    copy(columns = updated)
  }

  /**
   * Convert the current schema to a FrameSchema.
   *
   * This is useful when copying a Schema whose internals might be a VertexSchema
   * or EdgeSchema but you need to make sure it is a FrameSchema.
   */
  def toFrameSchema: FrameSchema = {
    if (isInstanceOf[FrameSchema]) {
      this.asInstanceOf[FrameSchema]
    }
    else {
      new FrameSchema(columns)
    }
  }

  /**
   * create a column name that is unique, suitable for adding to the schema
   * (subject to race conditions, only provides unique name for schema as
   * currently defined)
   * @param candidate a candidate string to start with, an _N number will be
   *                  append to make it unique
   * @return unique column name for this schema, as currently defined
   */
  def getNewColumnName(candidate: String): String = {
    var newName = candidate
    var i: Int = 0
    while (columnNames.contains(newName)) {
      newName = newName + s"_$i"
      i += 1
    }
    newName
  }

}
