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

package org.trustedanalytics.atk.engine.frame.plugins.load

import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.trustedanalytics.atk.domain.frame.load.{ LineParser, LineParserArguments }
import org.trustedanalytics.atk.domain.schema.Column
import org.trustedanalytics.atk.domain.schema.DataTypes
import org.trustedanalytics.atk.domain.schema._
import org.trustedanalytics.atk.engine.EngineConfig
import org.trustedanalytics.atk.engine.frame._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.frame.FrameRdd
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.Text
import org.trustedanalytics.atk.engine.frame.plugins.load.TextPlugin._
import org.trustedanalytics.atk.engine.plugin.Invocation

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Helper functions for loading an RDD
 */
object LoadRddFunctions extends Serializable {

  /**
   * Schema for Error Frames
   */
  val ErrorFrameSchema = new FrameSchema(List(Column("original_row", DataTypes.str), Column("error_message", DataTypes.str)))

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param parser the parser
   * @param minPartitions minimum number of partitions for text file.
   * @param startTag Start tag for XML or JSON parsers
   * @param endTag Start tag for XML or JSON parsers
   * @param isXml True for XML input files
   * @return RDD of Row objects
   */
  def loadAndParseLines(sc: SparkContext,
                        fileName: String,
                        parser: LineParser,
                        minPartitions: Option[Int] = None,
                        startTag: Option[List[String]] = None,
                        endTag: Option[List[String]] = None,
                        isXml: Boolean = false): ParseResultRddWrapper = {

    val fileContentRdd: RDD[String] =
      startTag match {
        case Some(s) =>
          val conf = new org.apache.hadoop.conf.Configuration()
          conf.setStrings(MultiLineTaggedInputFormat.START_TAG_KEY, s: _*) //Treat s as a Varargs parameter
          val e = endTag.get
          conf.setStrings(MultiLineTaggedInputFormat.END_TAG_KEY, e: _*)
          conf.setBoolean(MultiLineTaggedInputFormat.IS_XML_KEY, isXml)
          sc.newAPIHadoopFile[LongWritable, Text, MultiLineTaggedInputFormat](fileName, classOf[MultiLineTaggedInputFormat], classOf[LongWritable], classOf[Text], conf)
            .map(row => row._2.toString).filter(_.trim() != "")
        case None =>
          val rdd = minPartitions match {
            case Some(partitions) => sc.textFile(fileName, partitions)
            case _ => sc.textFile(fileName)
          }
          rdd.filter(_.trim() != "")
      }

    if (parser != null) {

      // parse a sample so we can bail early if needed
      //parseSampleOfData(fileContentRdd, parser)

      // re-parse the entire file
      parse(fileContentRdd, parser)
    }
    else {
      val listColumn = List(Column("data_lines", DataTypes.str))
      val rows = fileContentRdd.map(s => Row(s))
      ParseResultRddWrapper(new FrameRdd(new FrameSchema(listColumn), rows), null)
    }

  }

  /**
   * Load each line from client data into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param data data to parse
   * @param parser parser provided
   * @return  RDD of Row objects
   */
  def loadAndParseData(sc: SparkContext,
                       data: List[List[Any]],
                       parser: LineParser): ParseResultRddWrapper = {
    val dataContentRDD: RDD[Any] = sc.parallelize(data)
    // parse a sample so we can bail early if needed
    parseSampleOfData(dataContentRDD, parser)

    // re-parse the entire file
    parse(dataContentRDD, parser)
  }

  /**
   * Union the additionalData onto the end of the existingFrame
   * @param existingFrame the target DataFrame that may or may not already have data
   * @param additionalData the data to add to the existingFrame
   * @return the frame with updated schema
   */
  def unionAndSave(existingFrame: SparkFrame, additionalData: FrameRdd)(implicit invocation: Invocation): SparkFrame = {
    existingFrame.save(existingFrame.rdd.union(additionalData))
  }

  /**
   * Parse a sample of the file so we can bail early if a certain threshold fails.
   *
   * Throw an exception if too many rows can't be parsed.
   *
   * @param fileContentRdd the rows that need to be parsed (the file content)
   * @param parser the parser to use
   */
  private[frame] def parseSampleOfData[T: ClassTag](fileContentRdd: RDD[T],
                                                    parser: LineParser): Unit = {

    //parse the first number of lines specified as sample size and make sure the file is acceptable
    val sampleSize = EngineConfig.frameLoadTestSampleSize
    val threshold = EngineConfig.frameLoadTestFailThresholdPercentage

    val sampleRdd = MiscFrameFunctions.getPagedRdd[T](fileContentRdd, 0, sampleSize, sampleSize)

    //cache the RDD since it will be used multiple times
    sampleRdd.cache()

    val preEvaluateResults = parse(sampleRdd, parser)
    val failedCount = preEvaluateResults.errorLines.count()
    val sampleRowsCount: Long = sampleRdd.count()

    val failedRatio: Long = if (sampleRowsCount == 0) 0 else 100 * failedCount / sampleRowsCount

    //don't need it anymore
    sampleRdd.unpersist()

    if (failedRatio >= threshold) {
      val errorExampleRecord = preEvaluateResults.errorLines.first().copy()
      val errorRow = errorExampleRecord { 0 }
      val errorMessage = errorExampleRecord { 1 }
      throw new Exception(s"Parse failed on $failedCount rows out of the first $sampleRowsCount, " +
        s" please ensure your schema is correct.\nExample record that parser failed on : $errorRow    " +
        s" \n$errorMessage")
    }
  }

  /**
   * Parse rows and separate into successes and failures
   * @param rowsToParse the rows that need to be parsed (the file content)
   * @param parser the parser to use
   * @return the parse result - successes and failures
   */
  private[frame] def parse[T](rowsToParse: RDD[T], parser: LineParser)(implicit m: scala.reflect.ClassTag[T]): ParseResultRddWrapper = {

    val schemaArgs = parser.arguments.schema
    val skipRows = parser.arguments.skip_rows
    val parserFunction = getLineParser(parser, schemaArgs.columns.map(_._2).toArray)

    val parseResultRdd = rowsToParse.zipWithIndex.filter(_._2 >= skipRows.getOrElse(0)).map { _._1 }.mapPartitionsWithIndex { case (partition, lines) => lines.map(parserFunction) }

    try {
      parseResultRdd.cache()
      val successesRdd = parseResultRdd.filter(rowParseResult => rowParseResult.parseSuccess)
        .map(rowParseResult => rowParseResult.row)
      val failuresRdd = parseResultRdd.filter(rowParseResult => !rowParseResult.parseSuccess)
        .map(rowParseResult => rowParseResult.row)

      val schema = parser.arguments.schema
      new ParseResultRddWrapper(FrameRdd.toFrameRdd(schema.schema, successesRdd), FrameRdd.toFrameRdd(ErrorFrameSchema, failuresRdd))
    }
    finally {
      parseResultRdd.unpersist(blocking = false)
    }
  }

  private[frame] def getLineParser[T](parser: LineParser, columnTypes: Array[DataTypes.DataType]): T => RowParseResult = {
    parser.name match {
      //TODO: look functions up in a table rather than switching on names
      case "builtin/line/separator" =>
        val args = parser.arguments match {
          //TODO: genericize this argument conversion
          case a: LineParserArguments => a
          case x => throw new IllegalArgumentException(
            "Could not convert instance of " + x.getClass.getName + " to  arguments for builtin/line/separator")
        }
        val rowParser = new CsvRowParser(args.separator, columnTypes)
        s => rowParser(s.asInstanceOf[String])
        case "builtin/upload" =>
        val uploadParser = new UploadParser(columnTypes)
        row => uploadParser(row.asInstanceOf[List[Any]])
        case x => throw new Exception("Unsupported parser: " + x)
    }
  }

}
