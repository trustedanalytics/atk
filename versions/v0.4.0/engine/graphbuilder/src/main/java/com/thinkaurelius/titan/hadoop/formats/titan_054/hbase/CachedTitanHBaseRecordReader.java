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

package com.thinkaurelius.titan.hadoop.formats.titan_054.hbase;

import org.trustedanalytics.atk.graphbuilder.titan.cache.TitanHadoopCacheConfiguration;
import org.trustedanalytics.atk.graphbuilder.titan.cache.TitanHadoopGraphCache;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseHadoopGraph;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * A copy of the TitanHBaseRecordReader that supports cached Titan connections.
 * <p/>
 * This code is a copy of TitanHBaseRecordReader in Titan 0.5.4 with an added graph cache.
 * The class variables in TitanHBaseRecordReader were declared as private so we could not simply
 * extend the class and modify the record reader.
 *
 * @link https://github.com/thinkaurelius/titan/issues/817
 * <p/>
 * @see com.thinkaurelius.titan.hadoop.formats.hbase.TitanHBaseRecordReader
 * <p/>
 * This code also ensures that Titan connections are closed when errors occur to work-around
 * Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
 * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
 * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1.
 * The bug was fixed in Titan 0.5.2, but it still shows up occasionally if a task fails due to an exception.
 * We did not upgrade due to an issue in Titan 0.5.2 that caused bulk reads to hang on large datasets.
 */
public class CachedTitanHBaseRecordReader extends RecordReader<NullWritable, FaunusVertex> {
    private static TitanHadoopGraphCache graphCache = null;
    private static final Logger log =
            LoggerFactory.getLogger(CachedTitanHBaseRecordReader.class);

    static {
        graphCache = new TitanHadoopGraphCache();

        Runtime.getRuntime().addShutdownHook(new Thread() { //Needed to shutdown any runaway threads
            @Override
            public void run() {
                log.info("Invalidating Titan/HBase graph cache");
                invalidateGraphCache();
            }
        });
    }


    private RecordReader<ImmutableBytesWritable, Result> reader;
    private CachedTitanHBaseInputFormat inputFormat;
    private TitanHBaseHadoopGraph graph;
    private FaunusVertexQueryFilter vertexQuery;
    private com.thinkaurelius.titan.diskstorage.configuration.Configuration configuration;

    private FaunusVertex vertex;

    private final byte[] edgestoreFamilyBytes;

    public CachedTitanHBaseRecordReader(final CachedTitanHBaseInputFormat inputFormat,
                                        final FaunusVertexQueryFilter vertexQuery,
                                        final RecordReader<ImmutableBytesWritable, Result> reader,
                                        final byte[] edgestoreFamilyBytes) {
        this.inputFormat = inputFormat;
        this.vertexQuery = vertexQuery;
        this.reader = reader;
        this.edgestoreFamilyBytes = edgestoreFamilyBytes;
    }

    @Override
    public void initialize(final InputSplit inputSplit,
                           final TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(
                DEFAULT_COMPAT.getContextConfiguration(taskAttemptContext));
        TitanHadoopCacheConfiguration cacheConfiguration = new TitanHadoopCacheConfiguration(faunusConf);
        TitanHadoopSetup titanSetup = graphCache.getGraph(cacheConfiguration);

        graph = new TitanHBaseHadoopGraph(titanSetup);
        //graph = new TitanHBaseHadoopGraph(inputFormat.getGraphSetup());
        reader.initialize(inputSplit, taskAttemptContext);
        configuration = faunusConf;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (reader.nextKeyValue()) {
            final FaunusVertex temp = graph.readHadoopVertex(
                    configuration,
                    reader.getCurrentKey().copyBytes(),
                    reader.getCurrentValue().getMap().get(edgestoreFamilyBytes));
            if (null != temp) {
                vertex = temp;
                vertexQuery.filterRelationsOf(vertex);
                return true;
            }
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public FaunusVertex getCurrentValue() throws IOException, InterruptedException {
        return this.vertex;
    }

    @Override
    public void close() throws IOException {
        //Closing the graph is managed by the graph cache
        //this.graph.close();
        this.reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException  {
        return this.reader.getProgress();
    }

    /**
     * Invalidate all entries in the graph cache when the JVM shuts down.
     * This prevents any run-away message puller threads from maintaining a connection to the key-value store.
     */
    public static void invalidateGraphCache() {
        if (graphCache != null) {
            graphCache.invalidateAllCacheEntries();
        }
    }
}
