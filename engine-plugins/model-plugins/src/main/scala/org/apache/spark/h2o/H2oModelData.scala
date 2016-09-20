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

import java.io.{ FilenameFilter, FileWriter, BufferedWriter, File }
import java.net.{ URL, URLClassLoader }
import java.nio.file.Files
import javax.tools.ToolProvider
import hex.genmodel.GenModel
import hex.tree.drf.DRFModel
import org.trustedanalytics.atk.moduleloader.Module
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
    var genModel: GenModel = null
    try {
      val pojoUrl = compilePojo.toURI.toURL
      val classLoader = new URLClassLoader(Array[URL](pojoUrl), this.getClass.getClassLoader)
      val clz = Class.forName(modelName, true, classLoader)
      genModel = clz.newInstance.asInstanceOf[GenModel]
    }
    catch {
      case e: Exception => {
        throw new RuntimeException("Could not compile H2O model pojo:", e)
      }
    }
    genModel
  }

  /**
   * Get list of class files for generated model
   * @return Class files
   */
  def getModelClassFiles: List[File] = {
    var files = Array.empty[File]
    try {
      val pojoDir = compilePojo
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

  /**
   * Compile POJO
   * @return Directory with compiled classes
   */
  private def compilePojo: File = {
    val tmpDir = Files.createTempDirectory(modelName)
    tmpDir.toFile.deleteOnExit()

    // write pojo to temporary file
    val pojoFile = new File(tmpDir + "/" + modelName + ".java")
    val pojoOutput = new BufferedWriter(new FileWriter(pojoFile))
    pojoOutput.write(pojo)
    pojoOutput.close()

    // compile pojo
    val pojoDir = pojoFile.getParentFile
    val compiler = ToolProvider.getSystemJavaCompiler
    val paths = Module.libs.map(_.getPath).filter(_.contains("h2o")).mkString(":")
    compiler.run(null, null, null, "-cp", paths, pojoFile.getPath)
    pojoDir
  }
}
