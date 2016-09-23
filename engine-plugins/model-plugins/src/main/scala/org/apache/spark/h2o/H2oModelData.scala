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
package org.apache.spark.h2o

import java.io.{ BufferedWriter, File, FileWriter, FilenameFilter }
import java.net.{ URL, URLClassLoader }
import java.nio.file.Files
import javax.tools.ToolProvider

import com.google.common.cache.{ CacheBuilder, CacheLoader, RemovalListener, RemovalNotification }
import hex.genmodel.GenModel
import hex.tree.drf.DRFModel
import org.apache.commons.io.FileUtils
import water.util.JCodeGen

/**
 * Trained H2O model data
 *
 * @param modelName H2O model name
 * @param pojo POJO (Plain Old Java Object)
 * @param labelColumn Label column in train frame
 * @param observationColumns Observation columns in train frame
 */
case class H2oModelData(modelName: String, pojo: String, labelColumn: String, observationColumns: List[String]) {

  /**
   * H2O model data contructor using H2O random forest moel
   *
   * @param drfModel H2O random forest moel
   * @param labelColumn Label column in train frame
   * @param observationColumns Observation columns in train frame
   */
  def this(drfModel: DRFModel, labelColumn: String, observationColumns: List[String]) = {
    this(JCodeGen.toJavaId(drfModel._key.toString), drfModel.toJava(false, false), labelColumn, observationColumns)
  }

  /**
   * Generate model from POJO
   * @return Generated model
   */
  def toGenModel: GenModel = {
    H2oModelCache.getGenmodel(this)
  }

  /**
   * Get list of class files for generated model
   * @return Class files
   */
  def getModelClassFiles: List[File] = {
    H2oModelCache.getClassFiles(this)
  }
}

object H2oModelCache {

  case class GeneratedClass(genModel: GenModel, genModelDir: File)

  /**
   * A cache of generated classes.
   *
   * From the Guava Docs: A Cache is similar to ConcurrentMap, but not quite the same. The most
   * fundamental difference is that a ConcurrentMap persists all elements that are added to it until
   * they are explicitly removed. A Cache on the other hand is generally configured to evict entries
   * automatically, in order to constrain its memory footprint.  Note that this cache does not use
   * weak keys/values and thus does not respond to memory pressure.
   */
  private val cache = CacheBuilder.newBuilder()
    .maximumSize(20)
    .removalListener(new RemovalListener[H2oModelData, GeneratedClass] {
      override def onRemoval(rm: RemovalNotification[H2oModelData, GeneratedClass]): Unit = {
        deleteDir(rm.getValue.genModelDir)
      }
    })
    .build(
      new CacheLoader[H2oModelData, GeneratedClass]() {
        override def load(data: H2oModelData): GeneratedClass = {
          var genModelDir: File = null
          var genModel: GenModel = null
          try {
            genModelDir = compilePojo(data)
            val pojoUrl = genModelDir.toURI.toURL
            val classLoader = new URLClassLoader(Array[URL](pojoUrl), this.getClass.getClassLoader)
            val clz = Class.forName(data.modelName, true, classLoader)
            genModel = clz.newInstance.asInstanceOf[GenModel]
            GeneratedClass(genModel, genModelDir)
          }
          catch {
            case e: Exception => {
              deleteDir(genModelDir)
              throw new RuntimeException("Could not compile H2O model pojo:", e)
            }
          }
        }
      })

  def getGenmodel(data: H2oModelData): GenModel = cache.get(data).genModel

  def getClassFiles(data: H2oModelData): List[File] = {
    var files = Array.empty[File]
    try {
      val pojoDir = cache.get(data).genModelDir
      files = pojoDir.listFiles(new FilenameFilter() {
        @Override
        def accept(dir: File, name: String): Boolean = {
          name.endsWith(".class")
        }
      })
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Could not compile H2O model pojo:", e)
      }
    }
    files.toList
  }

  private def deleteDir(file: File): Unit = {
    if (file == null) return
    val contents = file.listFiles()
    if (contents != null) {
      contents.foreach(f => deleteDir(f))
    }
    FileUtils.deleteQuietly(file)
  }

  /**
   * Compile POJO
   * @return Directory with compiled classes
   */
  private def compilePojo(data: H2oModelData): File = {
    val tmpDir = Files.createTempDirectory(data.modelName)

    // write pojo to temporary file
    val pojoFile = new File(tmpDir + "/" + data.modelName + ".java")
    val pojoOutput = new BufferedWriter(new FileWriter(pojoFile))
    pojoOutput.write(data.pojo)
    pojoOutput.close()

    // compile pojo
    val pojoDir = pojoFile.getParentFile
    val compiler = ToolProvider.getSystemJavaCompiler
    val libs = this.getClass.getClassLoader.asInstanceOf[URLClassLoader].getURLs
    val paths = libs.map(_.getPath).filter(_.contains("h2o")).mkString(":")
    compiler.run(null, null, null, "-cp", paths, pojoFile.getPath)
    pojoDir
  }
}