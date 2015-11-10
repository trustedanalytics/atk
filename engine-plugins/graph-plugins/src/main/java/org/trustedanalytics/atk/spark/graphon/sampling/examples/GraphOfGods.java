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


package com.trustedanalytics.spark.graphon.sampling.examples;

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.example.GraphOfTheGodsFactory;

/**
 * Creates the Titan Graph of the Gods graph in Titan
 */
public class GraphOfGods {

    public static void main(String[] args) {

        SerializableBaseConfiguration titanConfig = new SerializableBaseConfiguration();
        titanConfig.setProperty("storage.backend", "hbase");
        titanConfig.setProperty("storage.hbase.table", "graphofgods");
        titanConfig.setProperty("storage.hostname", "fairlane");

        TitanGraph graph = TitanFactory.open(titanConfig);
        GraphOfTheGodsFactory.load(graph);
        graph.commit();
    }
}
