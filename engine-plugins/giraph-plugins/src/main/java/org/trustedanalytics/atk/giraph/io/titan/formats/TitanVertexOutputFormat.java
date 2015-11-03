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


package org.trustedanalytics.atk.giraph.io.titan.formats;

import org.trustedanalytics.atk.giraph.io.titan.TitanGraphWriter;
import org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanUtils;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.OUTPUT_VERTEX_PROPERTY_KEY_LIST;
import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.TITAN_MAX_VERTICES_PER_COMMIT;

public abstract class TitanVertexOutputFormat<I extends WritableComparable,
        V extends Writable, E extends Writable> extends TextVertexOutputFormat<I, V, E> {

    protected static final Logger LOG = Logger.getLogger(TitanVertexOutputFormat.class);


    /**
     * set up Titan based on users' configuration
     *
     * @param conf : Giraph configuration
     */
    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<I, V, E> conf) {
        GiraphTitanUtils.setupTitanOutput(conf);
        super.setConf(conf);
    }

    /**
     * Create Titan vertex writer
     *
     * @param context Job context
     * @return Titan vertex writer
     */

    public abstract TextVertexWriter createVertexWriter(TaskAttemptContext context);

    /**
     * Abstract class to be implemented by the user to write Giraph results to Titan via BluePrint API
     */
    protected abstract class TitanVertexWriterToEachLine extends TextVertexWriterToEachLine {

        /**
         * Vertex value properties to filter
         */
        protected String[] vertexValuePropertyKeyList = null;

        /**
         * TitanFactory to write back results
         */
        protected TitanGraph graph = null;

        /**
         * Used to commit vertices in batches
         */
        protected int verticesPendingCommit = 0;

        /**
         * regular expression of the deliminators for a property list
         */
        protected String regexp = "[\\s,\\t]+";

        /**
         * Initialize Titan vertex writer and open graph
         * @param context Task attempt context
         */
        @Override
        public void initialize(TaskAttemptContext context) throws IOException,
                InterruptedException {
            super.initialize(context);
            this.graph = TitanGraphWriter.getGraphFromCache(context.getConfiguration());
            vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(context.getConfiguration()).split(regexp);
        }

        /**
         * Write results to Titan vertex
         *
         * @param vertex Giraph vertex
         * @return   Text line to be written
         * @throws IOException
         */
        @Override
        public abstract Text convertVertexToLine(Vertex<I, V, E> vertex) throws IOException;

        public void commitVerticesInBatches() {
            this.verticesPendingCommit++;
            if (this.verticesPendingCommit % TITAN_MAX_VERTICES_PER_COMMIT == 0) {
                this.graph.commit();
                LOG.info("Committed " + this.verticesPendingCommit + " vertices to TianGraph");
            }
        }

        /**
         * Shutdown Titan graph
         *
         * @param context Task attempt context
         * @throws IOException
         */
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            this.graph.commit();
            //Closing the graph is managed by the graph cache
            //this.graph.shutdown();
            //LOG.info(CLOSED_GRAPH);
            super.close(context);
        }
    }
}
