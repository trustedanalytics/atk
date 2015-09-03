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

package org.apache.spark.frame

import org.apache.spark.atk.graph.{ EdgeWrapper, VertexWrapper }
import org.apache.spark.frame.ordering.MultiColumnOrdering
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{ GenericMutableRow, GenericRow }
import org.apache.spark.sql.types.{ ArrayType, BooleanType, ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructField, StructType, TimestampType }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, types => SparkType }
import org.apache.spark.{ Partition, TaskContext }
import org.trustedanalytics.atk.domain.schema.DataTypes._
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.engine.frame.{ MiscFrameFunctions, RowWrapper }
import org.trustedanalytics.atk.engine.graph.plugins.exportfromtitan.{ EdgeHolder, EdgeSchemaAggregator, VertexSchemaAggregator }
import org.trustedanalytics.atk.graphbuilder.elements.{ GBEdge, GBVertex }

import scala.reflect.ClassTag

/**
 * A Frame RDD is a SchemaRDD with our version of the associated schema.
 *
 * This is our preferred format for loading frames as RDDs.
 *
 * @param frameSchema  the schema describing the columns of this frame
 */
class FrameRdd(val frameSchema: Schema, val prev: RDD[Row])
    extends RDD[Row](prev) {

  // Represents self in FrameRdd and its sub-classes
  type Self = this.type

  /**
   * A Frame RDD is a SchemaRDD with our version of the associated schema
   *
   * @param schema  the schema describing the columns of this frame
   * @param dataframe an existing schemaRDD that this FrameRdd will represent
   */
  def this(schema: Schema, dataframe: DataFrame) = this(schema, dataframe.rdd)

  /* Number of columns in frame */
  val numColumns = frameSchema.columns.size

  /** This wrapper provides richer API for working with Rows */
  val rowWrapper = new RowWrapper(frameSchema)

  /**
   * Convert this FrameRdd into an RDD of type Array[Any].
   *
   * This was added to support some legacy plugin code.
   */
  @deprecated("use FrameRdd and Rows instead")
  def toRowRdd: RDD[Array[Any]] = {
    mapRows(_.toArray)
  }

  /**
   * Spark schema representation of Frame Schema to be used with Spark SQL APIs
   */
  lazy val sparkSchema = FrameRdd.schemaToStructType(frameSchema)

  /**
   * Convert a FrameRdd to a Spark Dataframe
   * @return Dataframe representing the FrameRdd
   */
  def toDataFrame = new SQLContext(this.sparkContext).createDataFrame(this, sparkSchema)

  def toDataFrameUsingHiveContext = new org.apache.spark.sql.hive.HiveContext(this.sparkContext).createDataFrame(this, sparkSchema)

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    firstParent[Row].iterator(split, context)

  /**
   * overrides the default behavior so new partitions get created in the sorted order to maintain the data order for the
   * user
   *
   * @return an array of partitions
   */
  override def getPartitions: Array[org.apache.spark.Partition] = {
    firstParent[Row].partitions
  }

  /**
   * Convert FrameRdd into RDD[Vector] format required by MLLib
   */
  def toVectorDenseRDD(featureColumnNames: List[String]): RDD[Vector] = {
    this.mapRows(row => {
      val array = row.valuesAsArray(featureColumnNames, flattenInputs = true)
      val b = array.map(i => DataTypes.toDouble(i))
      Vectors.dense(b)
    })
  }

  def toDenseVectorRDDWithWeights(featureColumnNames: List[String], columnWeights: List[Double]): RDD[Vector] = {
    require(columnWeights.length == featureColumnNames.length, "Length of columnWeights and featureColumnNames needs to be the same")
    this.mapRows(row => {
      val array = row.valuesAsArray(featureColumnNames).map(row => DataTypes.toDouble(row))
      val columnWeightsArray = columnWeights.toArray
      val doubles = array.zip(columnWeightsArray).map { case (x, y) => x * y }
      Vectors.dense(doubles)
    }
    )
  }

  def toVectorRDD(featureColumnNames: List[String]) = {
    this mapRows (row => {
      val features = row.values(featureColumnNames).map(value => DataTypes.toDouble(value))
      Vectors.dense(features.toArray)
    })
  }

  /**
   * Spark map with a rowWrapper
   */
  def mapRows[U: ClassTag](mapFunction: (RowWrapper) => U): RDD[U] = {
    this.map(sqlRow => {
      mapFunction(rowWrapper(sqlRow))
    })
  }

  /**
   * Spark flatMap with a rowWrapper
   */
  def flatMapRows[U: ClassTag](mapFunction: (RowWrapper) => TraversableOnce[U]): RDD[U] = {
    this.flatMap(sqlRow => {
      mapFunction(rowWrapper(sqlRow))
    })
  }

  /**
   * Spark groupBy with a rowWrapper
   */
  def groupByRows[K: ClassTag](function: (RowWrapper) => K): RDD[(K, scala.Iterable[Row])] = {
    this.groupBy(row => {
      function(rowWrapper(row))
    })
  }

  def selectColumn(columnName: String): FrameRdd = {
    selectColumns(List(columnName))
  }

  /**
   * Spark keyBy with a rowWrapper
   */
  def keyByRows[K: ClassTag](function: (RowWrapper) => K): RDD[(K, Row)] = {
    this.keyBy(row => {
      function(rowWrapper(row))
    })
  }

  /**
   * Create a new FrameRdd that is only a subset of the columns of this FrameRdd
   * @param columnNames names to include
   * @return the FrameRdd with only some columns
   */
  def selectColumns(columnNames: List[String]): FrameRdd = {
    if (columnNames.isEmpty) {
      throw new IllegalArgumentException("list of column names can't be empty")
    }
    new FrameRdd(frameSchema.copySubset(columnNames), mapRows(row => row.valuesAsRow(columnNames)))
  }

  /**
   * Select a subset of columns while renaming them
   * @param columnNamesWithRename map of old names to new names
   * @return the new FrameRdd
   */
  def selectColumnsWithRename(columnNamesWithRename: Map[String, String]): FrameRdd = {
    if (columnNamesWithRename.isEmpty) {
      throw new IllegalArgumentException("map of column names can't be empty")
    }
    val preservedOrderColumnNames = frameSchema.columnNames.filter(name => columnNamesWithRename.contains(name))
    new FrameRdd(frameSchema.copySubsetWithRename(columnNamesWithRename), mapRows(row => row.valuesAsRow(preservedOrderColumnNames)))
  }

  /* Please see documentation. Zip works if 2 SchemaRDDs have the same number of partitions
     and same number of elements in  each partition */
  def zipFrameRdd(frameRdd: FrameRdd): FrameRdd = {
    new FrameRdd(frameSchema.addColumns(frameRdd.frameSchema.columns), this.zip(frameRdd).map { case (a, b) => Row.merge(a, b) })
  }

  /**
   * Drop columns - create a new FrameRdd with the columns specified removed
   */
  def dropColumns(columnNames: List[String]): FrameRdd = {
    convertToNewSchema(frameSchema.dropColumns(columnNames))
  }

  /**
   * Drop all columns with the 'ignore' data type.
   *
   * The ignore data type is a slight hack for ignoring some columns on import.
   */
  def dropIgnoreColumns(): FrameRdd = {
    convertToNewSchema(frameSchema.dropIgnoreColumns())
  }

  /**
   * Remove duplicate rows by specifying the unique columns
   */
  def dropDuplicatesByColumn(columnNames: List[String]): Self = {
    val numColumns = frameSchema.columns.size
    val columnIndices = frameSchema.columnIndices(columnNames)
    val otherColumnNames = frameSchema.columnNamesExcept(columnNames)
    val otherColumnIndices = frameSchema.columnIndices(otherColumnNames)

    val duplicatesRemovedRdd: RDD[Row] = this.mapRows(row => otherColumnNames match {
      case Nil => (row.values(columnNames), Nil)
      case _ => (row.values(columnNames), row.values(otherColumnNames))
    }).reduceByKey((x, y) => x).map {
      case (keyRow, valueRow) =>
        valueRow match {
          case Nil => new GenericRow(keyRow.toArray)
          case _ => {
            //merge and re-order entries to match schema
            val row = new GenericMutableRow(numColumns)
            for (i <- keyRow.indices) row.update(columnIndices(i), keyRow(i))
            for (i <- valueRow.indices) row.update(otherColumnIndices(i), valueRow(i))
            row
          }
        }
    }
    update(duplicatesRemovedRdd)
  }

  /**
   * Update rows in the frame
   *
   * @param newRows New rows
   * @return New frame with updated rows
   */
  def update(newRows: RDD[Row]): Self = {
    (new FrameRdd(this.frameSchema, newRows)).asInstanceOf[Self]
  }

  /**
   * Union two Frame's, merging schemas if needed
   */
  def union(other: FrameRdd): FrameRdd = {
    val unionedSchema = frameSchema.union(other.frameSchema)
    val part1 = convertToNewSchema(unionedSchema).prev
    val part2 = other.convertToNewSchema(unionedSchema).prev
    val unionedRdd = part1.union(part2)
    new FrameRdd(unionedSchema, unionedRdd)
  }

  def renameColumn(oldName: String, newName: String): FrameRdd = {
    new FrameRdd(frameSchema.renameColumn(oldName, newName), this)
  }

  /**
   * Add/drop columns to make this frame match the supplied schema
   *
   * @param updatedSchema the new schema to take effect
   * @return the new RDD
   */
  def convertToNewSchema(updatedSchema: Schema): FrameRdd = {
    if (frameSchema == updatedSchema) {
      // no changes needed
      this
    }
    else {
      // map to new schema
      new FrameRdd(updatedSchema, mapRows(row => row.valuesForSchema(updatedSchema)))
    }
  }

  /**
   * Sort by one or more columns
   * @param columnNamesAndAscending column names to sort by, true for ascending, false for descending
   * @return the sorted Frame
   */
  def sortByColumns(columnNamesAndAscending: List[(String, Boolean)]): FrameRdd = {
    require(columnNamesAndAscending != null && columnNamesAndAscending.nonEmpty, "one or more columnNames is required")

    val columnNames = columnNamesAndAscending.map(_._1)
    val ascendingPerColumn = columnNamesAndAscending.map(_._2)

    implicit val multiColumnOrdering = new MultiColumnOrdering(ascendingPerColumn)

    // ascending is always true here because we control in the ordering
    val sortedRows = this.sortBy(row => rowWrapper(row).values(columnNames), ascending = true)
    new FrameRdd(frameSchema, sortedRows)
  }

  /**
   * Create a new RDD where unique ids are assigned to a specified value.
   * The ids will be long values that start from a specified value.
   * The ids are inserted into a specified column. if it does not exist the column will be created.
   *
   * (used for _vid and _eid in graphs but can be used elsewhere)
   *
   * @param columnName column to insert ids into (adding if needed)
   * @param startId  the first id to add (defaults to 0), incrementing from there
   */
  def assignUniqueIds(columnName: String, startId: Long = 0): FrameRdd = {
    val sumsAndCounts: Map[Int, (Long, Long)] = MiscFrameFunctions.getPerPartitionCountAndAccumulatedSum(this)

    val newRows: RDD[Row] = this.mapPartitionsWithIndex((i, rows) => {
      val sum: Long = if (i == 0) 0L
      else sumsAndCounts(i - 1)._2
      val partitionStart = sum + startId
      rows.zipWithIndex.map {
        case (row: Row, index: Int) =>
          val id: Long = partitionStart + index
          rowWrapper(row).addOrSetValue(columnName, id)
      }
    })

    val newSchema: Schema = if (!frameSchema.hasColumn(columnName)) {
      frameSchema.addColumn(columnName, DataTypes.int64)
    }
    else
      frameSchema

    new FrameRdd(newSchema, newRows)
  }

  /**
   * Convert Vertex or Edge Frames to plain data frames
   */
  def toPlainFrame: FrameRdd = {
    new FrameRdd(frameSchema.toFrameSchema, this)
  }

  /**
   * Save this RDD to disk or other store
   * @param absolutePath location to store
   * @param storageFormat "file/parquet", "file/sequence", etc.
   */
  def save(absolutePath: String, storageFormat: String = "file/parquet"): Unit = {
    storageFormat match {
      case "file/sequence" => this.saveAsObjectFile(absolutePath)
      case "file/parquet" => this.toDataFrame.saveAsParquetFile(absolutePath)
      case format => throw new IllegalArgumentException(s"Unrecognized storage format: $format")
    }
  }

}

/**
 * Static Methods for FrameRdd mostly deals with
 */
object FrameRdd {

  def toFrameRdd(schema: Schema, rowRDD: RDD[Array[Any]]) = {
    new FrameRdd(schema, FrameRdd.toRowRDD(schema, rowRDD))
  }

  /**
   * Convert an RDD of mixed vertex types into a map where the keys are labels and values are FrameRdd's
   * @param gbVertexRDD Graphbuilder Vertex RDD
   * @return  keys are labels and values are FrameRdd's
   */
  def toFrameRddMap(gbVertexRDD: RDD[GBVertex]): Map[String, FrameRdd] = {

    import org.trustedanalytics.atk.graphbuilder.driver.spark.rdd.GraphBuilderRddImplicits._

    // make sure all of the vertices have a label
    val labeledVertices = gbVertexRDD.labelVertices(Nil)

    // figure out the schemas of the different vertex types
    val vertexSchemaAggregator = new VertexSchemaAggregator(Nil)
    val vertexSchemas = labeledVertices.aggregate(vertexSchemaAggregator.zeroValue)(vertexSchemaAggregator.seqOp, vertexSchemaAggregator.combOp).values

    vertexSchemas.map(schema => {
      val filteredVertices: RDD[GBVertex] = labeledVertices.filter(v => {
        v.getProperty(GraphSchema.labelProperty) match {
          case Some(p) => p.value == schema.label
          case _ => throw new RuntimeException(s"Vertex didn't have a label property $v")
        }
      })
      val vertexWrapper = new VertexWrapper(schema)
      val rows = filteredVertices.map(gbVertex => vertexWrapper.create(gbVertex))
      val frameRdd = new FrameRdd(schema.toFrameSchema, rows)

      (schema.label, frameRdd)
    }).toMap
  }

  /**
   * Convert an RDD of mixed vertex types into a map where the keys are labels and values are FrameRdd's
   * @param gbEdgeRDD Graphbuilder Edge RDD
   * @param gbVertexRDD Graphbuilder Vertex RDD
   * @return  keys are labels and values are FrameRdd's
   */
  def toFrameRddMap(gbEdgeRDD: RDD[GBEdge], gbVertexRDD: RDD[GBVertex]): Map[String, FrameRdd] = {

    val edgeHolders = gbEdgeRDD.map(edge => EdgeHolder(edge, null, null))
    val edgeSchemas = edgeHolders.aggregate(EdgeSchemaAggregator.zeroValue)(EdgeSchemaAggregator.seqOp, EdgeSchemaAggregator.combOp).values

    edgeSchemas.map(schema => {
      val filteredEdges: RDD[GBEdge] = gbEdgeRDD.filter(_.label == schema.label)
      val edgeWrapper = new EdgeWrapper(schema)
      val rows = filteredEdges.map(gbEdge => edgeWrapper.create(gbEdge))
      val frameRdd = new FrameRdd(schema.toFrameSchema, rows)

      (schema.label, frameRdd)
    }).toMap
  }

  /**
   * Converts row object from an RDD[Array[Any]] to an RDD[Product] so that it can be used to create a SchemaRDD
   * @return RDD[org.apache.spark.sql.Row] with values equal to row object
   */
  def toRowRDD(schema: Schema, rows: RDD[Array[Any]]): RDD[org.apache.spark.sql.Row] = {
    val rowRDD: RDD[org.apache.spark.sql.Row] = rows.map(row => {
      val mutableRow = new GenericMutableRow(row.length)
      row.zipWithIndex.map {
        case (o, i) =>
          o match {
            case null => null
            case _ =>
              val colType = schema.column(i).dataType
              mutableRow(i) = o.asInstanceOf[colType.ScalaType]

          }
      }
      mutableRow
    })
    rowRDD
  }

  def toIndexedRowRdd(indexedRows: RDD[(Long, org.apache.spark.sql.Row)], frameSchema: Schema, featureColumnNames: List[String]): RDD[IndexedRow] = {
    val rowWrapper = new RowWrapper(frameSchema)
    indexedRows.map {
      case (index, row) =>
        val array = rowWrapper(row).valuesAsArray(featureColumnNames, flattenInputs = true)
        val b = array.map(i => DataTypes.toDouble(i))
        IndexedRow(index, Vectors.dense(b))
    }
  }

  /**
   * Defines a VectorType "StructType" for SchemaRDDs
   */
  val VectorType = ArrayType(DoubleType, containsNull = false)

  /**
   * Converts the schema object to a StructType for use in creating a SchemaRDD
   * @return StructType with StructFields corresponding to the columns of the schema object
   */
  def schemaToStructType(schema: Schema): StructType = {
    val fields: Seq[StructField] = schema.columns.map {
      column =>
        StructField(column.name.replaceAll("\\s", ""), column.dataType match {
          case x if x.equals(DataTypes.int32) => IntegerType
          case x if x.equals(DataTypes.int64) => LongType
          case x if x.equals(DataTypes.float32) => FloatType
          case x if x.equals(DataTypes.float64) => DoubleType
          case x if x.equals(DataTypes.string) => StringType
          case x if x.isVector => VectorType
          case x if x.equals(DataTypes.ignore) => StringType
        }, nullable = true)
    }
    StructType(fields)
  }

  /**
   * Converts the spark DataTypes to our schema Datatypes
   * @return our schema DataType
   */
  def sparkDataTypeToSchemaDataType(dataType: org.apache.spark.sql.types.DataType): org.trustedanalytics.atk.domain.schema.DataTypes.DataType = {
    val intType = IntegerType.getClass
    val longType = LongType.getClass
    val floatType = FloatType.getClass
    val doubleType = DoubleType.getClass
    val stringType = StringType.getClass
    val dateType = DateType.getClass
    val timeStampType = TimestampType.getClass
    val byteType = ByteType.getClass
    val booleanType = BooleanType.getClass
    val decimalType = classOf[DecimalType] // DecimalType.getClass return value (DecimalType$) differs from expected DecimalType
    val shortType = ShortType.getClass

    val a = dataType.getClass
    a match {
      case `intType` => int32
      case `longType` => int64
      case `floatType` => float32
      case `doubleType` => float64
      case `decimalType` => float64
      case `shortType` => int32
      case `stringType` => DataTypes.string
      case `dateType` => DataTypes.string
      case `byteType` => int32
      case `booleanType` => int32
      case `timeStampType` => DataTypes.string
      case _ => throw new IllegalArgumentException(s"unsupported type $a")
    }
  }

  /**
   * Converts a spark dataType (as string)to our schema Datatype
   * @param sparkDataType spark data type
   * @return a DataType
   */
  def sparkDataTypeToSchemaDataType(sparkDataType: String): DataType = {
    if ("intType".equalsIgnoreCase(sparkDataType)) { int32 }
    else if ("longType".equalsIgnoreCase(sparkDataType)) { int64 }
    else if ("floatType".equalsIgnoreCase(sparkDataType)) { float32 }
    else if ("doubleType".equalsIgnoreCase(sparkDataType)) { float64 }
    else if ("decimalType".equalsIgnoreCase(sparkDataType)) { float64 }
    else if ("shortType".equalsIgnoreCase(sparkDataType)) { int32 }
    else if ("stringType".equalsIgnoreCase(sparkDataType)) { DataTypes.string }
    else if ("dateType".equalsIgnoreCase(sparkDataType)) { DataTypes.string }
    else if ("byteType".equalsIgnoreCase(sparkDataType)) { int32 }
    else if ("booleanType".equalsIgnoreCase(sparkDataType)) { int32 }
    else if ("timeStampType".equalsIgnoreCase(sparkDataType)) { DataTypes.string }
    else throw new IllegalArgumentException(s"unsupported type $sparkDataType")
  }

  /**
   * Converts the schema object to a StructType for use in creating a SchemaRDD
   * @return StructType with StructFields corresponding to the columns of the schema object
   */
  def schemaToHiveType(schema: Schema): List[(String, String)] = {
    val fields = schema.columns.map {
      column =>
        (column.name.replaceAll("\\s", ""), column.dataType match {
          case x if x.equals(DataTypes.int32) => "int"
          case x if x.equals(DataTypes.int64) => "long"
          case x if x.equals(DataTypes.float32) => "double"
          case x if x.equals(DataTypes.float64) => "double"
          case x if x.equals(DataTypes.string) => "string"
          case x => throw new IllegalArgumentException(s"unsupported export type ${x.toString}")
        })
    }
    fields
  }

}
