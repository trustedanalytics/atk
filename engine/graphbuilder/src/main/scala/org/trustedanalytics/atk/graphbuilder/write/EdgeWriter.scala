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


package org.trustedanalytics.atk.graphbuilder.write

import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.write.dao.EdgeDAO
import org.trustedanalytics.atk.graphbuilder.elements.GBEdge
import org.trustedanalytics.atk.graphbuilder.write.dao.EdgeDAO
import com.tinkerpop.blueprints

/**
 * Write parsed Edges into a Blueprints compatible Graph database
 *
 * @param edgeDAO data access creating and updating Edges in the Graph database
 * @param append True to append to an existing graph
 */
class EdgeWriter(edgeDAO: EdgeDAO, append: Boolean) extends Serializable {

  def write(edge: GBEdge): blueprints.Edge = {
    if (append) {
      edgeDAO.updateOrCreate(edge)
    }
    else {
      edgeDAO.create(edge)
    }
  }

}
