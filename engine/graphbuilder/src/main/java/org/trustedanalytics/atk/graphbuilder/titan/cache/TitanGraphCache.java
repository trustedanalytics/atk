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


package org.trustedanalytics.atk.graphbuilder.titan.cache;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class caches standard Titan graphs so that multiple threads in a single JVM can share a Titan connection.
 * <p/>
 * Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX/Giraph
 * because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 * threads.
 */
public class TitanGraphCache extends AbstractTitanGraphCache<Configuration, TitanGraph> {

    private final Log LOG = LogFactory.getLog(TitanGraphCache.class);

    /**
     * Creates a cache for standard Titan graphs.
     * <p/>
     * The cache is implemented as a key-value pair of configuration objects and standard Titan graphs.
     * Configuration objects are considered equal if they contain the same set of properties.
     */
    public TitanGraphCache() {
        LOG.info("Creating cache for standard Titan graphs");
        CacheLoader<Configuration, TitanGraph> cacheLoader = createCacheLoader();
        RemovalListener<Configuration, TitanGraph> removalListener = createRemovalListener();
        this.cache = createCache(cacheLoader, removalListener);
    }

    /**
     * Creates a Titan graph for the corresponding Titan configuration if the graph does not exist in the cache.
     */
    private CacheLoader<Configuration, TitanGraph> createCacheLoader() {
        CacheLoader<Configuration, TitanGraph> cacheLoader = new CacheLoader<Configuration, TitanGraph>() {
            public TitanGraph load(Configuration config) {
                LOG.info("Loading a standard titan graph into the cache");
                return TitanFactory.open(config);
            }
        };
        return (cacheLoader);
    }

    /**
     * Shut down a Titan graph when it is evicted from the cache
     */
    private RemovalListener<Configuration, TitanGraph> createRemovalListener() {
        RemovalListener<Configuration, TitanGraph> removalListener = new RemovalListener<Configuration, TitanGraph>() {
            public void onRemoval(RemovalNotification<Configuration, TitanGraph> removal) {
                TitanGraph titanGraph = removal.getValue();
                if (titanGraph != null) {
                    LOG.info("Evicting a standard Titan graph from the cache: " + cache.stats());
                    titanGraph.shutdown();
                }
            }
        };
        return (removalListener);
    }

}
