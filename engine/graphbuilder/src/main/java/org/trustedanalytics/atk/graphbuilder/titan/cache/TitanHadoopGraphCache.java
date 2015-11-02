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
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This class caches Titan/Hadoop graphs so that multiple threads in a single JVM can share a Titan connection.
 * <p/>
 * Titan/Hadoop graphs are used when reading Titan graphs from the backend storage using Hadoop input formats.
 * Instantiating multiple Titan graph instances in a single JVM is a significant bottleneck for Spark/GraphX/Giraph
 * because the cost of instantiating a Titan connection is high, and it also leads to increased contention among
 * threads.
 */
public class TitanHadoopGraphCache extends AbstractTitanGraphCache<TitanHadoopCacheConfiguration, TitanHadoopSetup> {

    private final Log LOG = LogFactory.getLog(TitanGraphCache.class);

    /**
     * Creates a cache for Titan/Hadoop graphs.
     * <p/>
     * The cache is implemented as a key-value pair of configuration objects and Titan/Hadoop graphs.
     * Configuration objects are considered equal if they contain the same set of properties.
     * Titan Hadoop graphs are instantiated in the TitanHadoopSetup object.
     */
    public TitanHadoopGraphCache() {
        LOG.info("Creating cache for Titan/Hadoop graphs");
        CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> cacheLoader = createCacheLoader();
        RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> removalListener = createRemovalListener();
        this.cache = createCache(cacheLoader, removalListener);
    }

    /**
     * Creates a Titan/Hadoop graph for the corresponding Titan configuration if the graph does not exist in the cache.
     */
    private CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> createCacheLoader() {
        CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup> cacheLoader = new CacheLoader<TitanHadoopCacheConfiguration, TitanHadoopSetup>() {
            public TitanHadoopSetup load(TitanHadoopCacheConfiguration config) {
                LOG.info("Loading a Titan/Hadoop graph into the cache.");
                //Commenting Titan 0.5.2 API
                //return config.getTitanInputFormat().getGraphSetup();

                //Graph setup for Titan 0.5.1
                String inputFormatClassName = config.getInputFormatClassName();
                ModifiableHadoopConfiguration faunusConf = config.getFaunusConfiguration();
                TitanHadoopSetup titanHadoopSetup = ConfigurationUtil.instantiate(inputFormatClassName, new Object[]{faunusConf.getHadoopConfiguration()},
                        new Class[]{Configuration.class});
                return (titanHadoopSetup);
            }
        };
        return (cacheLoader);
    }


    /**
     * Shut down a Titan/Hadoop graph when it is removed from the cache
     */
    private RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> createRemovalListener() {
        RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup> removalListener = new RemovalListener<TitanHadoopCacheConfiguration, TitanHadoopSetup>() {
            public void onRemoval(RemovalNotification<TitanHadoopCacheConfiguration, TitanHadoopSetup> removal) {
                TitanHadoopSetup titanGraph = removal.getValue();
                if (titanGraph != null) {
                    LOG.info("Evicting a Titan/Hadoop graph from the cache");
                    titanGraph.close();
                }
            }
        };
        return (removalListener);
    }


}
