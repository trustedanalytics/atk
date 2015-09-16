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

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * TitanHBaseVertexInputFormatLongDoubleNull loads vertex
 * with <code>Long</code> vertex ID's,
 * <code>Double</code> vertex values,
 * and <code>Float</code> edge weights.
 */
public class TitanVertexInputFormatLongDoubleNull extends
        TitanVertexInputFormat<LongWritable, DoubleWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(TitanVertexInputFormatLongDoubleNull.class);

    /**
     * Create vertex reader for Titan vertices
     *
     * @param split   Input splits for HBase table
     * @param context Giraph task context
     * @return VertexReader Vertex reader for Giraph vertices
     * @throws IOException
     */
    @Override
    public VertexReader<LongWritable, DoubleWritable, NullWritable> createVertexReader(
            InputSplit split, TaskAttemptContext context) throws IOException {

        return new LongDoubleNullVertexReader(split, context);
    }

    /**
     * Uses the RecordReader to return HBase data
     */
    protected static class LongDoubleNullVertexReader extends TitanVertexReaderCommon<DoubleWritable, NullWritable> {

        /**
         * Constructs Giraph vertex reader to read Giraph vertices from Titan/HBase table.
         *
         * @param split   Input split from HBase table
         * @param context Giraph task context
         * @throws IOException
         */
        public LongDoubleNullVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
            super(split, context);
        }

        /**
         * Construct a Giraph vertex from a Faunus (Titan/Hadoop vertex).
         *
         * @param conf         Giraph configuration with property names, and edge labels to filter
         * @param faunusVertex Faunus vertex
         * @return Giraph vertex
         */
        @Override
        public Vertex<LongWritable, DoubleWritable, NullWritable> readGiraphVertex(
                final ImmutableClassesGiraphConfiguration conf, final FaunusVertex faunusVertex) {

            // Initialize Giraph vertex
            Vertex<LongWritable, DoubleWritable, NullWritable> newGiraphVertex = conf.createVertex();
            newGiraphVertex.initialize(new LongWritable(faunusVertex.getLongId()), new DoubleWritable(0));

            // Add vertex properties
            Iterator<TitanProperty> titanProperties = vertexBuilder.buildTitanProperties(faunusVertex);
            newGiraphVertex.setValue(getDoubleWritableProperty(titanProperties));

            // Add edges
            Iterator<TitanEdge> titanEdges = vertexBuilder.buildBlueprintsEdges(faunusVertex);
            addGiraphEdges(newGiraphVertex, faunusVertex, titanEdges);

            return (newGiraphVertex);
        }

        /**
         * Create Double writable from value of first Titan property in iterator
         *
         * @param titanProperties Iterator of Titan properties
         * @return Double writable containing value of first property in list
         */
        private DoubleWritable getDoubleWritableProperty(Iterator<TitanProperty> titanProperties) {
            double vertexValue = 0;
            if (titanProperties.hasNext()) {
                Object propertyValue = titanProperties.next().getValue();
                try {
                    vertexValue = Double.parseDouble(propertyValue.toString());
                } catch (NumberFormatException e) {
                    LOG.warn("Unable to parse double value for property: " + propertyValue);
                }
            }
            return (new DoubleWritable(vertexValue));
        }
    }
}
