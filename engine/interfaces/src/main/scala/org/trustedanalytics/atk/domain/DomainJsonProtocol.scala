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

import java.net.URI
import java.util

import org.trustedanalytics.atk.event.EventLogging
import org.trustedanalytics.atk.domain.command.{ CommandDoc, CommandPost, CommandDefinition }
import org.trustedanalytics.atk.domain.frame.{ UdfDependency, Udf }
import org.trustedanalytics.atk.domain.frame.load.{ LoadFrameArgs, LineParser, LoadSource, LineParserArguments }
import org.trustedanalytics.atk.domain.frame.partitioning.{ RepartitionArgs, CoalesceArgs }
import org.trustedanalytics.atk.domain.gc.{ GarbageCollectionArgs, GarbageCollectionEntry, GarbageCollection }
import org.trustedanalytics.atk.domain.model._
import org.trustedanalytics.atk.domain.frame.load._
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.engine.plugin.{ ApiMaturityTag, Call, Invocation }
import org.trustedanalytics.atk.engine.plugin.ApiMaturityTag.ApiMaturityTag

import spray.json._
import org.trustedanalytics.atk.domain.frame._
import org.trustedanalytics.atk.domain.graph._
import org.trustedanalytics.atk.domain.graph.construction._
import org.trustedanalytics.atk.domain.graph.{ GraphEntity, LoadGraphArgs, GraphReference, GraphTemplate }
import org.trustedanalytics.atk.domain.schema.DataTypes.DataType
import org.trustedanalytics.atk.domain.schema.{ DataTypes, Schema }
import org.joda.time.{ Duration, DateTime }
import org.trustedanalytics.atk.engine._

import scala.reflect.ClassTag
import scala.util.matching.Regex
import org.trustedanalytics.atk.spray.json._
import org.trustedanalytics.atk.UnitReturn

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.runtime.{ universe => ru }
import ru._
import scala.collection.mutable.ArrayBuffer

/**
 * Implicit conversions for domain objects to/from JSON
 *
 * NOTE: Order of implicits matters unless you give an explicit type on the left hand side.
 */
object DomainJsonProtocol extends AtkDefaultJsonProtocol with EventLogging {

  implicit object DataTypeFormat extends JsonFormat[DataTypes.DataType] {
    override def read(json: JsValue): DataType = {
      val raw = json.asInstanceOf[JsString].value
      DataTypes.toDataType(raw)
    }

    override def write(obj: DataType): JsValue = new JsString(obj.toString)
  }

  implicit val dateTimeFormat = new JsonFormat[DateTime] {
    private val dateTimeFmt = org.joda.time.format.ISODateTimeFormat.dateTime
    def write(x: DateTime) = JsString(dateTimeFmt.print(x))
    def read(value: JsValue) = value match {
      case JsString(x) => dateTimeFmt.parseDateTime(x)
      case x => deserializationError("Expected DateTime as JsString, but got " + x)
    }
  }

  implicit val durationFormat = new JsonFormat[Duration] {
    def write(x: Duration) = JsString(x.toString)
    def read(value: JsValue) = value match {
      case JsString(x) => Duration.parse(x)
      case x => deserializationError("Expected Duration as JsString, but got " + x)
    }
  }

  implicit val columnFormat: RootJsonFormat[Column] = jsonFormat3(Column)
  implicit val frameSchemaFormat: RootJsonFormat[FrameSchema] = jsonFormat(FrameSchema, "columns")
  implicit val vertexSchemaFormat: RootJsonFormat[VertexSchema] = jsonFormat(VertexSchema, "columns", "label", "id_column_name")
  implicit val edgeSchemaFormat: RootJsonFormat[EdgeSchema] = jsonFormat(EdgeSchema, "columns", "label", "src_vertex_label", "dest_vertex_label", "directed")
  implicit val schemaArgsFormat: RootJsonFormat[SchemaArgs] = jsonFormat1(SchemaArgs)

  /**
   * Format that can handle reading both the current schema class and the old one.
   */
  implicit object SchemaConversionFormat extends JsonFormat[Schema] {

    /** same format as the old one */
    case class LegacySchema(columns: List[(String, DataType)])
    implicit val legacyFormat = jsonFormat1(LegacySchema)

    override def write(obj: Schema): JsValue = obj match {
      case f: FrameSchema => frameSchemaFormat.write(f)
      case v: VertexSchema => vertexSchemaFormat.write(v)
      case e: EdgeSchema => edgeSchemaFormat.write(e)
      case _ => throw new IllegalArgumentException("New type not yet implemented: " + obj.getClass.getName)
    }

    /**
     * Read json
     */
    override def read(json: JsValue): Schema = {
      if (json.asJsObject.fields.contains("src_vertex_label")) {
        edgeSchemaFormat.read(json)
      }
      else if (json.asJsObject.fields.contains("label")) {
        vertexSchemaFormat.read(json)
      }
      else {
        frameSchemaFormat.read(json)
      }
    }
  }

  implicit object FileNameFormat extends JsonFormat[FileName] {
    override def write(obj: FileName): JsValue = JsString(obj.name)

    override def read(json: JsValue): FileName = json match {
      case JsString(name) => FileName(name)
      case x => deserializationError("Expected file name, but got " + x)
    }
  }

  /**
   * Holds a regular expression, plus the group number we care about in case
   * the pattern is a match
   */
  case class PatternIndex(pattern: Regex, groupNumber: Int) {
    def findMatch(text: String): Option[String] = {
      val result = pattern.findFirstMatchIn(text)
        .map(m => m.group(groupNumber))
        .flatMap(s => Option(s))
      result
    }
  }

  abstract class UriReferenceFormat[T <: UriReference]
      extends JsonFormat[T] {
    override def write(obj: T): JsValue = JsObject("uri" -> JsString(obj.uri))

    override def read(json: JsValue): T = {
      implicit val invocation: Invocation = Call(null, EngineExecutionContext.global)
      try {
        json match {
          case JsString(uri) => createReference(uri.substring(uri.lastIndexOf('/') + 1).toLong)
          case JsNumber(n) => createReference(n.toLong)
          case JsObject(o) => o("uri") match {
            case JsString(uri) => createReference(uri.substring(uri.lastIndexOf('/') + 1).toLong)
          }
          case JsNull => createReference(0) // todo: remove --needed to mark invalid model/new dummy reference, the real problem
        }
      }
      catch {
        case e: Exception => deserializationError(s"Expected valid URI, but received $json", e)
      }
    }

    def createReference(id: Long): T
    def createReference(uri: String): T
  }

  implicit val frameReferenceFormat = new UriReferenceFormat[FrameReference] {
    override def createReference(id: Long): FrameReference = id
    override def createReference(uri: String): FrameReference = uri
  }
  implicit def singletonOrListFormat[T: JsonFormat] = new JsonFormat[SingletonOrListValue[T]] {
    def write(list: SingletonOrListValue[T]) = JsArray(list.value.map(_.toJson))
    def read(value: JsValue): SingletonOrListValue[T] = value match {
      case JsArray(list) => SingletonOrListValue[T](list.map(_.convertTo[T]))
      case singleton => SingletonOrListValue[T](List(singleton.convertTo[T]))
    }
  }

  implicit val udfDependenciesFormat = jsonFormat2(UdfDependency)
  implicit val udfFormat = jsonFormat2(Udf)

  implicit object ApiMaturityTagFormat extends JsonFormat[ApiMaturityTag] {
    override def read(json: JsValue): ApiMaturityTag = json match {
      case JsString(value) => ApiMaturityTag.withName(value)
      case x => deserializationError(s"Expected string, received $x")
    }

    override def write(obj: ApiMaturityTag): JsValue = JsString(obj.toString)
  }

  /**
   * Convert Java collections to Json.
   */

  //These JSONFormats needs to be before the DataTypeJsonFormat which extends JsonFormat[Any]
  implicit def javaSetFormat[T: JsonFormat: TypeTag] = javaCollectionFormat[java.util.Set[T], T]
  implicit def javaListFormat[T: JsonFormat: TypeTag] = javaCollectionFormat[java.util.List[T], T]

  def javaCollectionFormat[E <: java.util.Collection[T]: TypeTag, T: JsonFormat: TypeTag]: JsonFormat[E] = new JsonFormat[E] {
    override def read(json: JsValue): E = json match {
      case JsArray(elements) =>
        val collection = typeOf[E] match {
          case t if t =:= typeOf[java.util.Set[T]] => new java.util.HashSet[T]()
          case t if t =:= typeOf[java.util.List[T]] => new util.ArrayList[T]()
          case x => deserializationError(s"Unable to deserialize Java collections of type $x")
        }
        val javaCollection = elements.map(_.convertTo[T]).asJavaCollection
        collection.addAll(javaCollection)
        collection.asInstanceOf[E]
      case x => deserializationError(s"Expected a Java collection, but received $x")
    }

    override def write(obj: E): JsValue = obj match {
      case javaCollection: java.util.Collection[T] => javaCollection.asScala.toJson
      case x => serializationError(s"Expected a Java collection, but received: $x")
    }
  }

  /**
   * Convert Java maps to Json.
   */
  //These JSONFormats needs to be before the DataTypeJsonFormat which extends JsonFormat[Any]
  implicit def javaHashMapFormat[K: JsonFormat, V: JsonFormat] = javaMapFormat[java.util.HashMap[K, V], K, V]
  implicit def javaUtilMapFormat[K: JsonFormat, V: JsonFormat] = javaMapFormat[java.util.Map[K, V], K, V]

  def javaMapFormat[M <: java.util.Map[K, V]: ClassTag, K: JsonFormat, V: JsonFormat]: JsonFormat[M] = new JsonFormat[M] {
    override def read(json: JsValue): M = json match {
      case x: JsObject =>
        val javaMap = new java.util.HashMap[K, V]()
        x.fields.map {
          case (key, value) =>
            javaMap.put(JsString(key).convertTo[K], value.convertTo[V])
        }
        javaMap.asInstanceOf[M]

      case x => deserializationError(s"Expected a Java map, but received $x")
    }

    override def write(obj: M): JsValue = obj match {
      case javaMap: java.util.Map[K, V] =>
        val map = javaMap.map {
          case (key, value) =>
            (key.toString, value.toJson)
        }.toMap
        JsObject(map)
      case x => serializationError(s"Expected a Java map, but received: $x")
    }
  }

  implicit object DataTypeJsonFormat extends JsonFormat[Any] {
    override def write(obj: Any): JsValue = {
      obj match {
        case n: Int => new JsNumber(n)
        case n: Long => new JsNumber(n)
        case n: Float => new JsNumber(BigDecimal(n))
        case n: Double => new JsNumber(n)
        case s: String => new JsString(s)
        case s: Boolean => JsBoolean(s)
        case v: ArrayBuffer[_] => new JsArray(v.map { case d: Double => JsNumber(d) }.toList) // for vector DataType
        case n: java.lang.Long => new JsNumber(n.longValue())
        case unk => serializationError("Cannot serialize " + unk.getClass.getName)
      }
    }

    override def read(json: JsValue): Any = {
      json match {
        case JsNumber(n) if n.isValidInt => n.intValue()
        case JsNumber(n) if n.isValidLong => n.longValue()
        case JsNumber(n) if n.isValidFloat => n.floatValue()
        case JsNumber(n) => n.doubleValue()
        case JsBoolean(b) => b
        case JsString(s) => s
        case unk => deserializationError("Cannot deserialize " + unk.getClass.getName)
      }
    }

  }
  implicit val longValueFormat = jsonFormat1(LongValue)
  implicit val intValueFormat = jsonFormat1(IntValue)
  implicit val stringValueFormat = jsonFormat1(StringValue)
  implicit val boolValueFormat = jsonFormat1(BoolValue)
  implicit val vectorValueFormat = jsonFormat1(VectorValue)

  implicit val createEntityArgsFormat = jsonFormat3(CreateEntityArgs.apply)

  implicit val userFormat = jsonFormat5(User)
  implicit val statusFormat = jsonFormat5(Status.apply)
  implicit val dataFrameTemplateFormat = jsonFormat2(DataFrameTemplate)
  implicit val separatorArgsJsonFormat = jsonFormat1(SeparatorArgs)
  implicit val definitionFormat = jsonFormat3(Definition)
  implicit val operationFormat = jsonFormat2(Operation)
  implicit val partialJsFormat = jsonFormat2(Partial[JsObject])
  implicit val loadLinesFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadLinesLongFormat = jsonFormat6(LoadLines[JsObject])
  implicit val loadSourceParserArgumentsFormat = jsonFormat3(LineParserArguments)
  implicit val loadSourceParserFormat = jsonFormat2(LineParser)
  implicit val loadSourceFormat = jsonFormat6(LoadSource)
  implicit val loadFormat = jsonFormat2(LoadFrameArgs)
  implicit val filterPredicateFormat = jsonFormat2(FilterArgs)
  implicit val removeColumnFormat = jsonFormat2(DropColumnsArgs)
  implicit val addColumnFormat = jsonFormat5(AddColumnsArgs)
  implicit val renameColumnsFormat = jsonFormat2(RenameColumnsArgs)
  implicit val groupByAggregationsFormat = jsonFormat3(GroupByAggregationArgs)
  implicit val groupByColumnFormat = jsonFormat3(GroupByArgs)
  implicit val copyWhereFormat = jsonFormat2(CountWhereArgs)

  implicit val errorFormat = jsonFormat5(Error)
  implicit val flattenColumnLongFormat = jsonFormat3(FlattenColumnArgs)
  implicit val unflattenColumnLongFormat = jsonFormat3(UnflattenColumnArgs)
  implicit val dropDuplicatesFormat = jsonFormat2(DropDuplicatesArgs)
  implicit val taskInfoFormat = jsonFormat1(TaskProgressInfo)
  implicit val progressInfoFormat = jsonFormat2(ProgressInfo)
  implicit val binColumnFormat = jsonFormat6(BinColumnArgs)
  implicit val computedBinColumnFormat = jsonFormat4(ComputedBinColumnArgs)
  implicit val sortByColumnsFormat = jsonFormat2(SortByColumnsArgs)

  implicit val columnSummaryStatisticsFormat = jsonFormat4(ColumnSummaryStatisticsArgs)
  implicit val columnSummaryStatisticsReturnFormat = jsonFormat13(ColumnSummaryStatisticsReturn)
  implicit val columnFullStatisticsFormat = jsonFormat3(ColumnFullStatisticsArgs)
  implicit val columnFullStatisticsReturnFormat = jsonFormat15(ColumnFullStatisticsReturn)

  implicit val categoricalColumnInput = jsonFormat3(CategoricalColumnInput)
  implicit val levelData = jsonFormat3(LevelData)
  implicit val categoricalColumnOutput = jsonFormat2(CategoricalSummaryOutput)
  implicit val categoricalSummaryArgsFormat = jsonFormat2(CategoricalSummaryArgs)
  implicit val categoricalSummaryReturnFormat = jsonFormat1(CategoricalSummaryReturn)

  implicit val computeMisplacedScoreInput = jsonFormat2(ComputeMisplacedScoreArgs)

  implicit val columnModeFormat = jsonFormat4(ColumnModeArgs)
  implicit val columnModeReturnFormat = jsonFormat4(ColumnModeReturn)

  implicit val columnMedianFormat = jsonFormat3(ColumnMedianArgs)
  implicit val columnMedianReturnFormat = jsonFormat1(ColumnMedianReturn)

  implicit val rowQueryFormat = jsonFormat3(RowQueryArgs[Long])

  implicit val cumulativeSumFormat = jsonFormat2(CumulativeSumArgs)
  implicit val cumulativePercentSumFormat = jsonFormat2(CumulativePercentArgs)
  implicit val cumulativeCountFormat = jsonFormat3(TallyArgs)
  implicit val cumulativePercentCountFormat = jsonFormat3(TallyPercentArgs)

  implicit val assignSampleFormat = jsonFormat5(AssignSampleArgs)
  implicit val calculatePercentilesFormat = jsonFormat3(QuantilesArgs)
  implicit val calculateCovarianceMatrix = jsonFormat3(CovarianceMatrixArgs)
  implicit val calculateCovariance = jsonFormat2(CovarianceArgs)
  implicit val covarianceReturnFormat = jsonFormat1(DoubleValue)

  implicit val calculateCorrelationMatrix = jsonFormat3(CorrelationMatrixArgs)
  implicit val calculateCorrelation = jsonFormat2(CorrelationArgs)

  implicit val entropyFormat = jsonFormat3(EntropyArgs)

  implicit val topKFormat = jsonFormat4(TopKArgs)
  implicit val exportHdfsCsvPlugin = jsonFormat5(ExportHdfsCsvArgs)
  implicit val exportHdfsJsonPlugin = jsonFormat4(ExportHdfsJsonArgs)
  implicit val exportHdfsHivePlugin = jsonFormat2(ExportHdfsHiveArgs)
  implicit val exportHdfsHBasePlugin = jsonFormat4(ExportHdfsHBaseArgs)

  //histogram formats
  implicit val histogramArgsFormat = jsonFormat5(HistogramArgs)
  implicit val histogramResultFormat = jsonFormat3(Histogram)

  // model performance formats

  implicit val classificationMetricLongFormat = jsonFormat5(ClassificationMetricArgs)
  implicit val classificationMetricValueLongFormat = jsonFormat5(ClassificationMetricValue)
  implicit val commandActionFormat = jsonFormat1(CommandPost)

  // model service formats
  implicit val ModelReferenceFormat = new UriReferenceFormat[ModelReference] {
    override def createReference(id: Long): ModelReference = id
    override def createReference(uri: String): ModelReference = uri
  }
  implicit val modelTemplateFormat = jsonFormat2(ModelTemplate)
  implicit val modelRenameFormat = jsonFormat2(RenameModelArgs)
  implicit val modelFormat = jsonFormat11(ModelEntity)
  implicit val genericNewModelArgsFormat = jsonFormat2(GenericNewModelArgs)

  // kmeans formats
  implicit val kmeansModelNewFormat = jsonFormat2(KMeansNewArgs)

  implicit val coalesceArgsFormat = jsonFormat3(CoalesceArgs)
  implicit val repartitionArgsFormat = jsonFormat2(RepartitionArgs)
  implicit val frameNoArgsFormat = jsonFormat1(FrameNoArgs)

  // graph service formats
  implicit val graphReferenceFormat = new UriReferenceFormat[GraphReference] {
    override def createReference(id: Long): GraphReference = id
    override def createReference(uri: String): GraphReference = uri
  }
  implicit val graphTemplateFormat = jsonFormat2(GraphTemplate)
  implicit val graphRenameFormat = jsonFormat2(RenameGraphArgs)

  implicit val graphNoArgsFormat = jsonFormat1(GraphNoArgs)

  implicit val schemaListFormat = jsonFormat1(SchemaList)

  // graph loading formats for specifying graphbuilder and graphload rules

  implicit val valueFormat = jsonFormat2(ValueRule)
  implicit val propertyFormat = jsonFormat2(PropertyRule)
  implicit val edgeRuleFormat = jsonFormat5(EdgeRule)
  implicit val vertexRuleFormat = jsonFormat2(VertexRule)
  implicit val frameRuleFormat = jsonFormat3(FrameRule)
  implicit val graphLoadFormat = jsonFormat3(LoadGraphArgs)
  implicit val quantileFormat = jsonFormat2(Quantile)
  implicit val QuantileCalculationResultFormat = jsonFormat1(QuantileValues)
  implicit val defineVertexFormat = jsonFormat2(DefineVertexArgs)
  implicit val defineEdgeFormat = jsonFormat5(DefineEdgeArgs)
  implicit val addVerticesFormat = jsonFormat4(AddVerticesArgs)
  implicit val addEdgesFormat = jsonFormat6(AddEdgesArgs)
  implicit val getAllGraphFramesFormat = jsonFormat1(GetAllGraphFrames)
  implicit val filterVertexRowsFormat = jsonFormat2(FilterVerticesArgs)
  implicit val copyGraphFormat = jsonFormat2(CopyGraphArgs)

  implicit val exportGraphFormat = jsonFormat2(ExportGraph)

  // garbage collection formats

  implicit val gcFormat = jsonFormat7(GarbageCollection)
  implicit val gcEntryFormat = jsonFormat7(GarbageCollectionEntry)

  implicit object UnitReturnJsonFormat extends RootJsonFormat[UnitReturn] {
    override def write(obj: UnitReturn): JsValue = {
      JsObject()
    }

    override def read(json: JsValue): UnitReturn = {
      throw new RuntimeException("UnitReturn type should never be provided as an argument")
    }
  }

  implicit object UriFormat extends JsonFormat[URI] {
    override def read(json: JsValue): URI = json match {
      case JsString(value) => new URI(value)
      case x => deserializationError(s"Expected string, received $x")
    }

    override def write(obj: URI): JsValue = JsString(obj.toString)
  }

  implicit object JsonSchemaFormat extends JsonFormat[JsonSchema] {
    override def read(json: JsValue): JsonSchema = json match {
      case JsObject(o) =>
        o.getOrElse("type", JsString("object")) match {
          case JsString("string") => stringSchemaFormat.read(json)
          case JsString("array") => arraySchemaFormat.read(json)
          case JsString("number") => numberSchemaFormat.read(json)
          case JsString("boolean") => booleanSchemaFormat.read(json)
          case _ => objectSchemaFormat.read(json)
        }
      case x => deserializationError(s"Expected a Json schema object, but got $x")
    }

    override def write(obj: JsonSchema): JsValue = obj match {
      case o: ObjectSchema => objectSchemaFormat.write(o)
      case s: StringSchema => stringSchemaFormat.write(s)
      case a: ArraySchema => arraySchemaFormat.write(a)
      case n: NumberSchema => numberSchemaFormat.write(n)
      case b: BooleanSchema => booleanSchemaFormat.write(b)
      case u: UnitSchema => unitSchemaFormat.write(u)
      case JsonSchema.empty => JsObject().toJson
      case x => serializationError(s"Expected a valid json schema object, but received: $x")
    }
  }

  lazy implicit val booleanSchemaFormat: RootJsonFormat[BooleanSchema] = jsonFormat5(BooleanSchema)
  lazy implicit val numberSchemaFormat: RootJsonFormat[NumberSchema] = jsonFormat10(NumberSchema)
  lazy implicit val stringSchemaFormat: RootJsonFormat[StringSchema] = jsonFormat10(StringSchema)
  lazy implicit val objectSchemaFormat: RootJsonFormat[ObjectSchema] = jsonFormat13(ObjectSchema)
  lazy implicit val arraySchemaFormat: RootJsonFormat[ArraySchema] = jsonFormat10(ArraySchema)
  lazy implicit val unitSchemaFormat: RootJsonFormat[UnitSchema] = jsonFormat4(UnitSchema)

  implicit object CommandDocFormat extends JsonFormat[CommandDoc] {
    override def read(value: JsValue): CommandDoc = {
      throw new NotImplementedError("CommandDoc JSON read") // We only dump these, no need to impl. read
    }

    override def write(doc: CommandDoc): JsValue = {
      val title = JsString(doc.oneLineSummary)
      val description = doc.extendedSummary match {
        case Some(d) => JsString(d)
        case None => JsNull
      }
      val examples = doc.examples match {
        case Some(e) => JsObject(e.map { case (x, y) => x -> JsString(y) })
        case None => JsNull
      }
      JsObject("title" -> title, "description" -> description, "examples" -> examples)
    }
  }

  /**
   * Explict JSON handling for FrameCopy where 'columns' arg can be a String, a List, or a Map
   */
  implicit object FrameCopyFormat extends JsonFormat[CopyFrameArgs] {
    override def read(value: JsValue): CopyFrameArgs = {
      val jo = value.asJsObject
      val frame = frameReferenceFormat.read(jo.getFields("frame").head)
      val columns: Option[Map[String, String]] = jo.getFields("columns") match {
        case Seq(JsString(n)) => Some(Map[String, String](n -> n))
        case Seq(JsArray(names)) => Some((for (n <- names) yield (n.convertTo[String], n.convertTo[String])).toMap)
        case Seq(JsObject(fields)) => Some(for ((name, new_name) <- fields) yield (name, new_name.convertTo[String]))
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON string, array, or object for argument 'columns' but got $x")
      }
      val where: Option[Udf] = jo.getFields("where") match {
        case Seq(JsObject(fields)) => Some(Udf(fields("function").convertTo[String], fields("dependencies").convertTo[List[UdfDependency]]))
        case Seq(JsNull) => None
        case Seq() => None
        case x => deserializationError(s"Expected FrameCopy JSON expression for argument 'where' but got $x")
      }
      val name: Option[String] = jo.getFields("name") match {
        case Seq(JsString(n)) => Some(n)
        case Seq(JsNull) => None
      }
      CopyFrameArgs(frame, columns, where, name)
    }

    override def write(frameCopy: CopyFrameArgs): JsValue = frameCopy match {
      case CopyFrameArgs(frame, columns, where, name) => JsObject("frame" -> frame.toJson,
        "columns" -> columns.toJson,
        "where" -> where.toJson,
        "name" -> name.toJson)
    }
  }

  lazy implicit val commandDefinitionFormat = jsonFormat5(CommandDefinition)

  implicit object dataFrameFormat extends JsonFormat[FrameEntity] {
    implicit val dataFrameFormatOriginal = jsonFormat19(FrameEntity.apply)

    override def read(value: JsValue): FrameEntity = {
      dataFrameFormatOriginal.read(value)
    }

    override def write(frame: FrameEntity): JsValue = {
      JsObject(dataFrameFormatOriginal.write(frame).asJsObject.fields +
        ("uri" -> JsString(frame.uri)) +
        ("entity_type" -> JsString(frame.entityType)))
    }
  }

  implicit object graphFormat extends JsonFormat[GraphEntity] {
    implicit val graphFormatOriginal = jsonFormat12(GraphEntity)

    override def read(value: JsValue): GraphEntity = {
      graphFormatOriginal.read(value)
    }

    override def write(graph: GraphEntity): JsValue = {
      JsObject(graphFormatOriginal.write(graph).asJsObject.fields +
        ("uri" -> JsString(graph.uri)) +
        ("entity_type" -> JsString(graph.entityType)))
    }
  }

  implicit val seamlessGraphMetaFormat = jsonFormat2(SeamlessGraphMeta)

  implicit val binColumnResultFormat = jsonFormat2(BinColumnResults)

  implicit val garbageCollectionArgsFormat = jsonFormat2(GarbageCollectionArgs)

  implicit val hBaseArgsSchemaFormat = jsonFormat3(HBaseSchemaArgs)
  implicit val hBaseArgsFormat = jsonFormat5(HBaseArgs)
  implicit val jdbcArgsFormat = jsonFormat5(JdbcArgs)

}
