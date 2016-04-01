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

package org.trustedanalytics.atk.engine.frame

import java.io.File
import java.util

import org.trustedanalytics.atk.moduleloader.ClassLoaderAware
import org.trustedanalytics.atk.domain.frame.{ FrameReference, Udf }
import org.trustedanalytics.atk.domain.schema.{ Column, DataTypes, Schema }
import org.trustedanalytics.atk.engine.plugin.Invocation
import org.trustedanalytics.atk.engine.{ SparkContextFactory, EngineConfig }
import org.apache.spark.{ Accumulator, SparkContext }
import org.apache.spark.api.python.{ AtkPythonBroadcast, EnginePythonAccumulatorParam, EnginePythonRdd }
import org.apache.commons.codec.binary.Base64.decodeBase64
import java.util.{ ArrayList => JArrayList, List => JList }

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.frame.FrameRdd
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.bson.types.BasicBSONList
import org.bson.{ BSON, BasicBSONObject }
import scala.collection.mutable.{ ListBuffer, ArrayBuffer }
import com.google.common.io.Files

import scala.collection.JavaConversions._

import scala.reflect.io.{ Directory, Path }

object PythonRddStorage {

  private def decodePythonBase64EncodedStrToBytes(byteStr: String): Array[Byte] = {
    decodeBase64(byteStr)
  }

  //TODO: Use config + UUID rather than hard coded paths.
  private def uploadUdfDependencies(udf: Udf): List[String] = {
    udf.dependencies.map(d => {
      val path = new File(EngineConfig.pythonUdfDependenciesDirectory)
      path.mkdirs() // no check --if false, dirs may already exist; if other problem, will catch during write
      val data = decodePythonBase64EncodedStrToBytes(d.fileContent)
      val fileName = d.fileName.split("/").last
      val fullFileName = EngineConfig.pythonUdfDependenciesDirectory + fileName
      try {
        Files.write(data, new File(fullFileName))
      }
      catch {
        case e: Exception => throw new Exception(s"Unable to upload UDF dependency '$fullFileName'  $e")
      }
      fileName
    })
  }

  def mapWith(data: FrameRdd, udf: Udf, udfSchema: Schema = null, sc: SparkContext): FrameRdd = {
    val newSchema = if (udfSchema == null) {
      data.frameSchema
    }
    else {
      udfSchema
    }
    val converter = DataTypes.parseMany(newSchema.columns.map(_.dataType).toArray)(_)
    val accumulatorSer = sc.accumulator(0L, "mytimerSerGeneric")
    val accumulatorDeSer = sc.accumulator(0L, "mytimerDeSerGeneric")
    val pyRdd = rddToPyRdd(udf, data, sc, accumulatorSer)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter, accumulatorDeSer)
    println(s"MytimerSer in mapWith took ${accumulatorSer.value}")
    frameRdd.count()
    println(s"MytimerDeSer in mapWith took ${accumulatorDeSer.value}")
    FrameRdd.toFrameRdd(newSchema, frameRdd)
  }

  /**
   * This method returns a FrameRdd after applying UDF on referencing FrameRdd
   *
   * @param data Current referencing FrameRdd
   * @param aggregateByColumnKeys List of column name(s) based on which yeahaggregation is performed
   * @param udf User Defined function(UDF) to apply on each row
   * @param udfSchema Mandatory output schema
   * @return FrameRdd
   */
  def aggregateMapWith(data: FrameRdd, aggregateByColumnKeys: List[String], udf: Udf, udfSchema: Schema, sc: SparkContext): FrameRdd = {
    //Create a new schema which includes keys (KeyedSchema).
    val keyedSchema = udfSchema.copy(columns = data.frameSchema.columns(aggregateByColumnKeys) ++ udfSchema.columns)
    //track key indices to fetch data during BSON decode.
    //val keyIndices = for (key <- aggregateByColumnKeys) yield data.frameSchema.columnIndex(key)
    val converter = DataTypes.parseMany(keyedSchema.columns.map(_.dataType).toArray)(_)

    val groupRDD = data.groupByRows(row => row.values(aggregateByColumnKeys))

    val accumulatorSer = sc.accumulator(0L, "mytimerSerAggregated")
    val accumulatorDeSer = sc.accumulator(0L, "mytimerDeSerAggregated")
    val pyRdd = aggregateRddToPyRdd(udf, groupRDD, sc, accumulatorSer)
    val frameRdd = getRddFromPythonRdd(pyRdd, converter, accumulatorDeSer)
    //serialization timer
    println(s"MytimerSer in AggregateUDF took ${accumulatorSer.value}")
    frameRdd.count()
    println(s"MytimerDeSer in AggregateUDF took ${accumulatorDeSer.value}")
    FrameRdd.toFrameRdd(keyedSchema, frameRdd)
  }

  //TODO: fix hardcoded paths
  def uploadFilesToSpark(uploads: List[String], sc: SparkContext): JArrayList[String] = {
    val pythonIncludes = new JArrayList[String]()
    if (uploads != null) {
      for (k <- uploads.indices) {
        sc.addFile(s"file://${EngineConfig.pythonUdfDependenciesDirectory}" + uploads(k))
        pythonIncludes.add(uploads(k))
      }
    }
    pythonIncludes
  }

  /**
   * Convert an iterable into a BasicBSONList for serialiation purposes
   *
   * @param iterable vector object
   */
  def iterableToBsonList(iterable: Iterable[Any]): BasicBSONList = {
    val bsonList = new BasicBSONList
    // BasicBSONList only supports the put operation with a numeric value as the key.
    iterable.zipWithIndex.foreach {
      case (obj: Any, index: Int) => bsonList.put(index, obj)
    }
    bsonList
  }

  def rddToPyRdd(udf: Udf, rdd: RDD[Row], sc: SparkContext, acc: Accumulator[Long] = null): EnginePythonRdd[Array[Byte]] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(udf.function)
    // Create an RDD of byte arrays representing bson objects
    val baseRdd: RDD[Array[Byte]] = rdd.map(
      x => {
        val start = System.nanoTime()
        val obj = new BasicBSONObject()
        obj.put("array", x.toSeq.toArray.map {
          case y: ArrayBuffer[_] => iterableToBsonList(y)
          case y: Vector[_] => iterableToBsonList(y)
          case y: scala.collection.mutable.Seq[_] => iterableToBsonList(y)
          case value => value
        })
        val res = BSON.encode(obj)
        println(s"Bson Encoded obj: ${res}")
        if (acc != null)
          acc += (System.nanoTime() - start)
        res
      }
    )
    println(s"RddToPyRddGeneric Bytes ${baseRdd.first()} ${baseRdd.first().length}")
    val pyRdd = getPyRdd(udf, sc, baseRdd, predicateInBytes)
    pyRdd
  }

  /**
   * This method converts the base RDD into Python RDD which is processed by the Python VM at the server.
   *
   * @param udf UDF provided by the user
   * @param baseRdd Base RDD in Array[Bytes]
   * @param predicateInBytes UDF in Array[Bytes]
   * @return pythonRDD
   */
  def getPyRdd(udf: Udf, sc: SparkContext, baseRdd: RDD[Array[Byte]], predicateInBytes: Array[Byte]): EnginePythonRdd[Array[Byte]] = {
    val pythonExec = EngineConfig.pythonWorkerExec
    val environment = new util.HashMap[String, String]()
    //This is needed to make the python executors put the spark jar (with the pyspark files) on the PYTHONPATH.
    //Without it, only the first added jar (engine.jar) goes on the pythonpath, and since engine.jar has
    //more than 65563 files, python can't import pyspark from it (see SPARK-1520 for details).
    //The relevant code in the Spark core project is in PythonUtils.scala. This code must use, e.g. the same
    //version number for py4j that Spark's PythonUtils uses.

    //TODO: Refactor Spark's PythonUtils to better support this use case
    val sparkPython: Path = (EngineConfig.sparkHome: Path) / "python"
    environment.put("PYTHONPATH",
      Seq(sparkPython,
        sparkPython / "lib" / "py4j-0.8.2.1-src.zip",
        //Support dev machines without installing python packages
        //TODO: Maybe hide behind a flag?
        Directory.Current.get / "python").map(_.toString()).mkString(":"))

    val accumulator = sc.accumulator[JList[Array[Byte]]](new JArrayList[Array[Byte]]())(new EnginePythonAccumulatorParam())
    val broadcastVars = new JArrayList[Broadcast[AtkPythonBroadcast]]()
    val pythonVersion = "2.7"

    val pyIncludes = new JArrayList[String]()

    sc.addFile(s"file://$pythonDepZip")
    pyIncludes.add("trustedanalytics.zip")

    if (udf.dependencies != null) {
      val includes = uploadUdfDependencies(udf)
      pyIncludes.addAll(uploadFilesToSpark(includes, sc))
    }

    val pyRdd = new EnginePythonRdd[Array[Byte]](
      baseRdd, predicateInBytes, environment,
      pyIncludes, preservePartitioning = true,
      pythonExec = pythonExec,
      pythonVer = pythonVersion,
      broadcastVars, accumulator)
    pyRdd
  }

  /**
   * This method encodes the raw rdd into Bson to convert into PythonRDD
   *
   * @param udf UDF provided by user to apply on each row
   * @param rdd rdd(List[keys], List[Rows])
   * @return PythonRdd
   */
  def aggregateRddToPyRdd(udf: Udf, rdd: RDD[(List[Any], Iterable[Row])], sc: SparkContext, acc: Accumulator[Long] = null): EnginePythonRdd[Array[Byte]] = {
    val predicateInBytes = decodePythonBase64EncodedStrToBytes(udf.function)
    val baseRdd: RDD[Array[Byte]] = rdd.map {
      case (key, rows) => {
        val x = System.nanoTime()
        val obj = new BasicBSONObject()
        val bsonRows = rows.map(
          row => {
            row.toSeq.toArray.map {
              case cell: ArrayBuffer[_] => iterableToBsonList(cell)
              case cell: Vector[_] => iterableToBsonList(cell)
              case cell: scala.collection.mutable.Seq[_] => iterableToBsonList(cell)
              case value => value
            }
          }).toArray
        //obj.put("keyindices", keyIndices.toArray)
        obj.put("array", bsonRows)
        val res = BSON.encode(obj)
        println(s"Bson Encoded obj: ${res}")
        val y = System.nanoTime()
        if (acc != null)
          acc += (y - x)
        res
      }
    }
    println(s"agg12-RddToPyRddAggregation Bytes ${baseRdd.first()} ${baseRdd.first().length}")
    val pyRdd = getPyRdd(udf, sc, baseRdd, predicateInBytes)
    pyRdd
  }

  /** Path to trustedanalytics.zip (our python code) */
  lazy val pythonDepZip: String = {
    getResourcePath("trustedanalytics.zip", EngineConfig.pythonDefaultDependencySearchDirectories)
      .getOrElse(throw new RuntimeException("Python dependencies were not packaged for UDF execution (searched: " + EngineConfig.pythonDefaultDependencySearchDirectories.mkString(", ") + ")"))
  }

  // TODO: this recursive searching is quite slow, there must be a better way to do this
  private def getResourcePath(resourceName: String, additionalPaths: Seq[String]): Option[String] = {
    val currentDirectory = Directory.Current.getOrElse(
      throw new RuntimeException(s"Error encountered while looking up $resourceName in current directory"))
    val searchableDirectories = Array(currentDirectory) ++ (for { path <- additionalPaths } yield Directory(path))

    val result = for {
      directory <- searchableDirectories
      file <- directory.deepFiles.toList.map(_.toString())
      if file.endsWith(resourceName)
    } yield file

    result.headOption
  }

  def getRddFromPythonRdd(pyRdd: EnginePythonRdd[Array[Byte]], converter: (Array[Any] => Array[Any]) = null, acc: Accumulator[Long] = null): RDD[Array[Any]] = {
    val resultRdd = pyRdd.flatMap(s => {
      val start = System.nanoTime()
      //should be BasicBSONList containing only BasicBSONList objects
      val bson = BSON.decode(s)
      val asList = bson.get("array").asInstanceOf[BasicBSONList]
      val res = asList.map(innerList => {
        val asBsonList = innerList.asInstanceOf[BasicBSONList]
        asBsonList.map {
          case x: BasicBSONList => x.toArray
          case value => value
        }.toArray.asInstanceOf[Array[Any]]
      })
      val end = System.nanoTime()
      if (acc != null)
        acc += (end - start)
      res
    }).map(converter)

    resultRdd
  }
}

/**
 * Loading and saving Python RDD's
 */
class PythonRddStorage(frames: SparkFrameStorage) extends ClassLoaderAware {

  /**
   * Create a Python RDD
   *
   * @param frameRef source frame for the parent RDD
   * @param py_expression Python expression encoded in Python's Base64 encoding (different than Java's)
   * @return the RDD
   */
  def createPythonRDD(frameRef: FrameReference, py_expression: String, sc: SparkContext)(implicit invocation: Invocation): EnginePythonRdd[Array[Byte]] = {
    withMyClassLoader {
      val frameEntity = frames.expectFrame(frameRef)
      val rdd = frames.loadFrameData(sc, frameEntity)
      PythonRddStorage.rddToPyRdd(new Udf(py_expression, null), rdd, sc)
    }
  }
}
