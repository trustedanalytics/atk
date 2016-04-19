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
package org.trustedanalytics.atk.plugins.orientdb

import org.trustedanalytics.atk.engine.plugin.ArgDoc
import scala.collection.immutable.Map

/**
 * Created by wtaie on 4/14/16.
 */

case class ExportOrientDbGraphReturn(@ArgDoc("""a tuple of two: the vertices class name and the corresponding number of exported Orient vertices.""") oVerticesStats: Map[String, Long],
                                     @ArgDoc("""a tuple of two: the edges class name and the corresponding number of exported Orient edges.""") oEdgeStats: Map[String, Long],
                                     @ArgDoc("""The created Orient database URI .""") dbUri: String)
