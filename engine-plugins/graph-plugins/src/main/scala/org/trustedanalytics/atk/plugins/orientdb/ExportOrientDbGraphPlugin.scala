/**
  * Copyright (c) 2015 Intel Corporation 
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *       http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.trustedanalytics.atk.plugins.orientdb

/**
  * Created by wtaie on 3/31/16.
  */

import org.trustedanalytics.atk.UnitReturn
import org.trustedanalytics.atk.engine.plugin.{ Invocation, PluginDoc, SparkCommandPlugin }
import org.trustedanalytics.atk.engine.plugin.ArgDoc

//Implicits needed for JSON conversion
import spray.json._
import org.trustedanalytics.atk.domain.DomainJsonProtocol._
object ExportOrientDbGraphJsonFormat{
  implicit val exportOrientDbGraphArgsFormat = jsonFormat5(ExportOrientDbGraphArgs)
}
import org.trustedanalytics.atk.plugins.orientdb.ExportOrientDbGraphJsonFormat._

/**
  * export graph to OrientDB
  */
@PluginDoc(oneLine = "export current graph as OrientDb graph",extended = "Export the graph to Orient database file in the provided dbUrl.")
class ExportOrientDbGraphPlugin extends SparkCommandPlugin[ExportOrientDbGraphArgs, UnitReturn] {

  /**
    * the name of the command
    *
    * @return
    */
  override def name: String = "graph:/export_to_orientdb"

  /**
    *
    * @param arguments the arguments supplied by the caller
    * @param invocation
    * @return a value of type declared as the Return type.
    */

  override def execute(arguments: ExportOrientDbGraphArgs)(implicit invocation: Invocation): UnitReturn = ???


}