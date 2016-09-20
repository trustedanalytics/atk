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

case class H2oModelData(modelName: String, pojo: String, labelColumn: String, observationColumns: List[String]) {

  def this(drfModel: DRFModel, labelColumn: String, observationColumns: List[String]) = {
    this(JCodeGen.toJavaId(drfModel._key.toString), drfModel.toJava(false, false), labelColumn, observationColumns)
  }

  def toGenModel: GenModel = {
    var genModel: GenModel = null
    try {
      val tmpDir = Files.createTempDirectory(modelName)
      tmpDir.toFile.deleteOnExit()

      // write pojo to temporary file
      val pojoFile = new File(tmpDir + "/" + modelName + ".java")
      val pojoOutput = new BufferedWriter(new FileWriter(pojoFile))
      pojoOutput.write(pojo)
      pojoOutput.close()

      // compile pojo
      val pojoUrl = pojoFile.getParentFile.toURI.toURL
      val compiler = ToolProvider.getSystemJavaCompiler
      val paths = Module.libs.map(_.getPath).filter(_.contains("h2o")).mkString(":")
      compiler.run(null, null, null, "-cp", paths, pojoFile.getPath)
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

  def getModelClassFiles: List[File] = {
    var files = Array.empty[File]
    try {
      val tmpDir = Files.createTempDirectory(modelName)
      tmpDir.toFile.deleteOnExit()

      // write pojo to temporary file
      val pojoFile = new File(tmpDir + "/" + modelName + ".java")
      val pojoOutput = new BufferedWriter(new FileWriter(pojoFile))
      pojoOutput.write(pojo)
      pojoOutput.close()

      // compile pojo
      val pojoUrl = pojoFile.getParentFile.toURI.toURL
      val compiler = ToolProvider.getSystemJavaCompiler
      val paths = Module.libs.map(_.getPath).filter(_.contains("h2o")).mkString(":")
      compiler.run(null, null, null, "-cp", paths, pojoFile.getPath)
      files = tmpDir.toFile.listFiles(new FilenameFilter() {
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
}
