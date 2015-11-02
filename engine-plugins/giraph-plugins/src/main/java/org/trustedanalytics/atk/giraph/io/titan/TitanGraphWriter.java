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


package org.trustedanalytics.atk.giraph.io.titan;

import org.trustedanalytics.atk.graphbuilder.titan.cache.TitanGraphCache;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.diskstorage.configuration.backend.CommonsConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;

/**
 * The titan graph writer which connects Giraph to Titan
 * for writing back algorithm results
 */
public class TitanGraphWriter {

    /**
     * Cache of Titan graph connections
     *
     * Cache Titan connections because the cost of instantiating multiple Titan graph instances
     * in a single JVM is a significant bottleneck for Spark/GraphX/Giraph since Titan 0.5.2+
     */
    private static TitanGraphCache graphCache = null;

    /**
     * LOG class
     */
    private static final Logger LOG = Logger
            .getLogger(TitanGraphWriter.class);

    /**
     * Ensures that Titan connections are closed when errors occur to work-around
     * Issue#817 KCVSLog$MessagePuller does not shut down when using the TitanInputFormat
     * The Spark context does no shut down due to a runaway KCVSLog$MessagePuller thread that maintained a
     * connection to the underlying graph. Affects Titan 0.5.0 and 0.5.1.
     * The bug was fixed in Titan 0.5.2, but it still shows up occasionally if a task fails due to an exception.
     *
     * @link https://github.com/thinkaurelius/titan/issues/817
     */
    static {
        graphCache = new TitanGraphCache();

        Runtime.getRuntime().addShutdownHook(new Thread() { //Needed to shutdown any runaway threads
            @Override
            public void run() {
                LOG.info("Invalidating Titan graph cache");
                invalidateGraphCache();
            }
        });
    }

    /**
     * Do not instantiate
     */
    private TitanGraphWriter() {
    }

    /**
     * Open Titan graph
     *
     * @param context task attempt context
     * @return Titan graph to which Giraph write results
     */
    public static TitanGraph open(TaskAttemptContext context) throws IOException {
        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(
                context.getConfiguration(),
                GIRAPH_TITAN.get(context.getConfiguration()));
        return getTitanGraph(baseConfig);
    }



    /**
     * Open Titan graph
     *
     * @param config Immutable Giraph Configuration
     * @return Titan graph to which Giraph write results
     */
    public static TitanGraph open(ImmutableClassesGiraphConfiguration config) throws IOException {
        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(
                config,
                GIRAPH_TITAN.get(config));
        return getTitanGraph(baseConfig);
    }

    /**
     * Get Titan graph from cache
     *
     * @param hadoopConfig Hadoop configuration
     * @return Titan graph to which Giraph write results
     */
    public static TitanGraph getGraphFromCache(Configuration hadoopConfig) {
        BaseConfiguration baseConfig = GiraphToTitanGraphFactory.createTitanBaseConfiguration(
                hadoopConfig, GIRAPH_TITAN.get(hadoopConfig));
        TitanGraph graph = graphCache.getGraph(baseConfig);
        return graph;
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

    // Returns connection to Titan graph
    private static TitanGraph getTitanGraph(BaseConfiguration baseConfig) {
        GraphDatabaseConfiguration titanConfig = new GraphDatabaseConfiguration(new CommonsConfiguration(baseConfig));
        return new StandardTitanGraph(titanConfig);
    }
}
