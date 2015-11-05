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


package org.trustedanalytics.atk.giraph.io.titan.common;

import org.trustedanalytics.atk.giraph.io.titan.TitanGraphWriter;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import org.apache.commons.lang.StringUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.*;


/**
 * Utility methods for Titan IO
 */
public class GiraphTitanUtils {
    private static final String propertyKeyRegExp = "[\\s,\\t]+";

    /**
     * Logger
     */
    private static final Logger LOG = Logger.getLogger(GiraphTitanUtils.class);

    /**
     * Do not instantiate
     */
    private GiraphTitanUtils() {
    }

    /**
     * check whether input parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public static void sanityCheckInputParameters(Configuration conf) {

        if (StringUtils.EMPTY.equals(GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf))) {
            throw new IllegalArgumentException(CONFIG_TITAN + "host name" + CONFIG_PREFIX +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + CONFIGURED_DEFAULT +
                    ENSURE_PORT + GIRAPH_TITAN_STORAGE_PORT.get(conf));

        }

        if (StringUtils.isEmpty(INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST.get(conf))) {
            LOG.info(NO_VERTEX_PROPERTY + ENSURE_INPUT_FORMAT);
        }

        if (StringUtils.isEmpty(INPUT_EDGE_VALUE_PROPERTY_KEY_LIST.get(conf))) {
            LOG.info(NO_EDGE_PROPERTY + ENSURE_INPUT_FORMAT);
        }

        if (StringUtils.isEmpty(INPUT_EDGE_LABEL_LIST.get(conf))) {
            LOG.info(NO_EDGE_LABEL + ENSURE_INPUT_FORMAT);
        }

        if (StringUtils.isEmpty(VERTEX_TYPE_PROPERTY_KEY.get(conf))) {
            LOG.info(NO_VERTEX_TYPE + ENSURE_INPUT_FORMAT);
        }

        if (StringUtils.isEmpty(EDGE_TYPE_PROPERTY_KEY.get(conf))) {
            LOG.info(NO_EDGE_TYPE + ENSURE_INPUT_FORMAT);
        }
    }

    /**
     * check whether output parameter is valid
     *
     * @param conf : Giraph configuration
     */
    public static void sanityCheckOutputParameters(ImmutableClassesGiraphConfiguration conf) {

        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(propertyKeyRegExp);
        if (vertexValuePropertyKeyList.length == 0) {
            throw new IllegalArgumentException(CONFIG_VERTEX_PROPERTY + CONFIG_PREFIX +
                    OUTPUT_VERTEX_PROPERTY_KEY_LIST.getKey() + NO_VERTEX_READ);
        }

        if (StringUtils.EMPTY.equals(GIRAPH_TITAN_STORAGE_BACKEND.get(conf))) {
            throw new IllegalArgumentException(CONFIG_TITAN + "backend" + CONFIG_PREFIX +
                    GIRAPH_TITAN_STORAGE_BACKEND.getKey() + NO_VERTEX_READ);
        }

        if (StringUtils.EMPTY.equals(GIRAPH_TITAN_STORAGE_HOSTNAME.get(conf))) {
            throw new IllegalArgumentException(CONFIG_TITAN + "host name" + CONFIG_PREFIX +
                    GIRAPH_TITAN_STORAGE_HOSTNAME.getKey() + NO_VERTEX_READ);
        }

        if (GIRAPH_TITAN_STORAGE_PORT.isDefaultValue(conf)) {
            LOG.info(GIRAPH_TITAN_STORAGE_PORT.getKey() + CONFIGURED_DEFAULT +
                    ENSURE_PORT + GIRAPH_TITAN_STORAGE_PORT.get(conf));
        }

        if ("true".equals(GIRAPH_TITAN_STORAGE_READ_ONLY.get(conf))) {
            throw new IllegalArgumentException(CONFIG_TITAN + "read only" + CONFIG_PREFIX +
                    GIRAPH_TITAN_STORAGE_READ_ONLY.getKey() + NO_VERTEX_READ);
        }

        if (StringUtils.EMPTY.equals(VERTEX_TYPE_PROPERTY_KEY.get(conf))) {
            LOG.info(NO_VERTEX_TYPE + ENSURE_INPUT_FORMAT);
        }

        if (StringUtils.EMPTY.equals(EDGE_TYPE_PROPERTY_KEY.get(conf))) {
            LOG.info(NO_EDGE_TYPE + ENSURE_INPUT_FORMAT);
        }
    }


    /**
     * @param conf : Giraph configuration
     * @TODO: Change this to match new schema
     * disable Hadoop speculative execution.
     */
    public static void createTitanKeys(ImmutableClassesGiraphConfiguration conf) {
        TitanGraph graph;

        //try {
            graph = TitanGraphWriter.getGraphFromCache(conf);
        //} catch (IOException e) {
       //     LOG.error(TITAN_GRAPH_NOT_OPEN);
       //     throw new RuntimeException(TITAN_GRAPH_NOT_OPEN);
       // }

        String[] vertexValuePropertyKeyList = OUTPUT_VERTEX_PROPERTY_KEY_LIST.get(conf).split(propertyKeyRegExp);

        TitanManagement graphManager = graph.getManagementSystem();

        for (int i = 0; i < vertexValuePropertyKeyList.length; i++) {
            if (!graphManager.containsRelationType(vertexValuePropertyKeyList[i])) {
                LOG.info(CREATE_VERTEX_PROPERTY + vertexValuePropertyKeyList[i]);
                //for titan 0.5.0+
                graphManager.makePropertyKey(vertexValuePropertyKeyList[i]).dataType(String.class).make();
            }
        }
        graphManager.commit();
        //graph.shutdown();
    }


    /**
     * disable Hadoop speculative execution.
     *
     * @param conf : Giraph configuration
     */
    public static void disableSpeculativeExe(ImmutableClassesGiraphConfiguration conf) {
        conf.setBoolean("mapreduce.map.tasks.speculative.execution", false);
        conf.setBoolean("mapreduce.reduce.tasks.speculative.execution", false);
    }

    /**
     * set up configuration for Titan Output
     *
     * @param conf : Giraph configuration
     */
    public static void setupTitanOutput(ImmutableClassesGiraphConfiguration conf) {
        sanityCheckOutputParameters(conf);
        createTitanKeys(conf);
        disableSpeculativeExe(conf);
    }
}
