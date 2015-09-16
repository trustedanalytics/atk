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

package org.trustedanalytics.atk.giraph.io.titan.formats;

import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.CURRENT_VERTEX;
import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.EXPECTED_SIZE_OF_VERTEX_PROPERTY;
import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.REAL_SIZE_OF_VERTEX_PROPERTY;
import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.VERTEX_PROPERTY_MISMATCH;

/**
 * The Vertex Output Format which writes back Giraph algorithm results
 * to Titan.
 * <p/>
 * Each Vertex is with <code>Long</code> id,
 * and <code>Long</code> values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class TitanVertexOutputFormatLongIDLongValue<I extends LongWritable,
    V extends LongWritable, E extends Writable>
    extends TitanVertexOutputFormat<I, V, E> {

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
        .getLogger(TitanVertexOutputFormatLongIDLongValue.class);

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TitanLongIDLongValueWriter();
    }

    /**
     * VertexWriter that writes Giraph results to Titan via BluePrint API
     * vertices with <code>Long</code> id
     * and <code>TwoVector</code> values.
     */
    protected class TitanLongIDLongValueWriter extends TitanVertexWriterToEachLine {

        @Override
        public Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException {

            long vertexId = vertex.getId().get();
            com.tinkerpop.blueprints.Vertex bluePrintVertex = this.graph.getVertex(vertexId);
            if (1 == vertexValuePropertyKeyList.length) {
                bluePrintVertex.setProperty(vertexValuePropertyKeyList[0], vertex.getValue().toString());
                //  LOG.info("saved " + vertexId);
            } else {
                LOG.error(VERTEX_PROPERTY_MISMATCH + EXPECTED_SIZE_OF_VERTEX_PROPERTY + "1" +
                    REAL_SIZE_OF_VERTEX_PROPERTY + vertexValuePropertyKeyList.length);
                throw new IllegalArgumentException(VERTEX_PROPERTY_MISMATCH +
                    CURRENT_VERTEX + vertex.getId());
            }
            commitVerticesInBatches();
            return null;
        }
    }
}
