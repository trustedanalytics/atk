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

package org.trustedanalytics.atk.giraph.io.titan.common;

import org.apache.giraph.conf.StrConfOption;


/**
 * Constants used all over Giraph for configuration specific for Titan/HBase
 * Titan/Cassandra
 */

public class GiraphTitanConstants {

    /**
     * Titan backend type .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_BACKEND = new StrConfOption(
        "giraph.titan.input.storage.backend", "", "Titan backend - required");
    /**
     * Titan Storage hostname .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_HOSTNAME = new StrConfOption(
        "giraph.titan.input.storage.hostname", "", "Titan/Hbase hostname - required");
    /**
     * Titan/HBase Storage table name .
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_HBASE_TABLE = new StrConfOption(
        "giraph.titan.input.storage.hbase.table", "", "Titan/Hbase tablename - required");
    /**
     * port where to contact Titan storage.
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_PORT = new StrConfOption(
        "giraph.titan.input.storage.port", "2181", "port where to contact Titan/hbase");
    /**
     * Titan storage batch loading.
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_BATCH_LOADING = new StrConfOption(
        "giraph.titan.input.storage.batch-loading", "true", "Titan storage batch-loading");
    /**
     * the configuration prefix to stripped for Titan
     */
    public static final StrConfOption GIRAPH_TITAN = new StrConfOption("giraph.titan", "giraph.titan.input",
        "Giraph/Titan prefix");
    /**
     * Storage backend
     */
    public static final StrConfOption GIRAPH_TITAN_STORAGE_READ_ONLY = new StrConfOption(
        "giraph.titan.input.storage.read-only", "false", "read only or not");
    /**
     * the list of vertex properties to filter during data loading from Titan
     */
    public static final StrConfOption INPUT_VERTEX_VALUE_PROPERTY_KEY_LIST = new StrConfOption(
        "input.vertex.value.property.key.list", "", "the vertex property keys which Giraph reads from Titan");
    /**
     * the list of edge properties to filter during data loading from Titan
     */
    public static final StrConfOption INPUT_EDGE_VALUE_PROPERTY_KEY_LIST = new StrConfOption(
        "input.edge.value.property.key.list", "", "the edge property keys which Giraph needs");
    /**
     * the list of edge labels to filter during data loading from titan
     */
    public static final StrConfOption INPUT_EDGE_LABEL_LIST = new StrConfOption("input.edge.label.list", "",
        "the edge labels which Giraph needs");
    /**
     * the list of vertex properties to write results back to Titan
     */
    public static final StrConfOption OUTPUT_VERTEX_PROPERTY_KEY_LIST = new StrConfOption(
        "output.vertex.property.key.list", "", "the vertex property keys which Giraph writes back to Titan");
    /**
     * the property key for vertex type
     */
    public static final StrConfOption VERTEX_TYPE_PROPERTY_KEY = new StrConfOption(
        "vertex.type.property.key", "", "the property key for vertex type");
    /**
     * the property key for edge type
     */
    public static final StrConfOption EDGE_TYPE_PROPERTY_KEY = new StrConfOption(
        "edge.type.property.key", "", "the property key for edge type");
    /**
     * whether to support a vector for vertex and edge value
     * when it is enabled, one vertex/edge value property value corresponds to
     * a vector as its value
     */
    public static final StrConfOption VECTOR_VALUE = new StrConfOption(
        "vector.value", "true", "whether to vertex property value for vertex and edge");

    /**
     * Maximum number of Titan vertices per commit,
     * Used to commit Titan vertices in batches.
     */
    public static int TITAN_MAX_VERTICES_PER_COMMIT = 20000;
    /**
     * create vertex property
     */
    public static final String CREATE_VERTEX_PROPERTY = "create vertex.property in Titan ";
    /**
     * failed to open titan graph
     */
    public static final String TITAN_GRAPH_NOT_OPEN = "GIRAPH ERROR: Unable to open Titan graph";
    /**
     * vertex property mismatch
     */
    public static final String VERTEX_PROPERTY_MISMATCH = "The number of output vertex property does not match! ";
    /**
     * expected size of vertex property
     */
    public static final String EXPECTED_SIZE_OF_VERTEX_PROPERTY = "The expected size of output vertex property is ";
    /**
     * current size of vertex property
     */
    public static final String REAL_SIZE_OF_VERTEX_PROPERTY = ", current size of output vertex property is ";
    /**
     * current vertex
     */
    public static final String CURRENT_VERTEX = "Current Vertex is: ";
    /**
     * No vertex read
     */
    public static final String NO_VERTEX_READ = ". Otherwise no vertex will be read from Titan.";
    /**
     * No vertex type
     */
    public static final String NO_VERTEX_TYPE = "No vertex type property specified. ";
    /**
     * No edge type
     */
    public static final String NO_EDGE_TYPE = "No edge type property specified. ";
    /**
     * No edge label
     */
    public static final String NO_EDGE_LABEL = "No input edge label specified. ";
    /**
     * No vertex property
     */
    public static final String NO_VERTEX_PROPERTY = "No vertex property list specified. ";
    /**
     * No edge property
     */
    public static final String NO_EDGE_PROPERTY = "No input edge property list specified. ";
    /**
     * ensure input format
     */
    public static final String ENSURE_INPUT_FORMAT = "Ensure your InputFormat does not require one.";
    /**
     * config titan
     */
    public static final String CONFIG_TITAN = "Please configure Titan storage ";
    /**
     * config vertex property
     */
    public static final String CONFIG_VERTEX_PROPERTY = "Please configure output vertex property list ";
    /**
     * config prefix
     */
    public static final String CONFIG_PREFIX = " by -D ";
    /**
     * ensure titan storage port
     */
    public static final String ENSURE_PORT = "Ensure you are using port ";
    /**
     * configured default
     */
    public static final String CONFIGURED_DEFAULT = " is configured as default value. ";
    /**
     * wrong vertex type
     */
    public static final String WRONG_VERTEX_TYPE = "Vertex type string: %s isn't supported.";
    /**
     * vertex type on the left side
     */
    public static final String VERTEX_TYPE_LEFT = "l";
    /**
     * vertex type on the right side
     */
    public static final String VERTEX_TYPE_RIGHT = "r";
    /**
     * edge type for training data
     */
    public static final String TYPE_TRAIN = "tr";
    /**
     * edge type for validation data
     */
    public static final String TYPE_VALIDATE = "va";
    /**
     * edge type for test data
     */
    public static final String TYPE_TEST = "te";

}
