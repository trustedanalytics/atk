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

package org.trustedanalytics.atk.graphbuilder.titan.cache;

import org.trustedanalytics.atk.graphbuilder.util.SerializableBaseConfiguration;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Map;

import static com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration.TITAN_INPUT_VERSION;

/**
 * Configuration for Titan/Hadoop graphs used to retrieve graphs from the Titan/Hadoop graph cache.
 * <p/>
 * The configuration object is used as the key to the graph caches, and retrieves Titan/Hadoop graphs
 * with the same set of Titan configuration properties.
 */
public class TitanHadoopCacheConfiguration {

    /**
     * Prefix that identifies Titan/Hadoop specific configurations
     */
    public static String TITAN_HADOOP_PREFIX = "titan.hadoop.input.conf.";

    /**
     * Class name for Titan Hadoop Setup
     */
    public static final String SETUP_PACKAGE_PREFIX = "com.thinkaurelius.titan.hadoop.formats.util.input.";
    public static final String SETUP_CLASS_NAME = ".TitanHadoopSetupImpl";

    /**
     * Titan configuration properties
     */
    private final SerializableBaseConfiguration titanConfig;

    /**
     * Titan/Hadoop configuration used to instantiate graph
     */
    private final ModifiableHadoopConfiguration faunusConf;

    /**
     * Titan/Hadoop input format class name
     */
    private final String inputFormatClassName;

    /**
     * Create cache configuration for Titan/Hadoop graphs
     *
     * @param faunusConf Titan/Hadoop (Faunus) configuration
     */
    public TitanHadoopCacheConfiguration(ModifiableHadoopConfiguration faunusConf) throws  IllegalArgumentException{
        if ( null == faunusConf) throw new IllegalArgumentException("Faunus configuration must not be null");

        this.faunusConf = faunusConf;
        this.inputFormatClassName = getInputFormatClassName();
        this.titanConfig = createTitanConfiguration(faunusConf.getHadoopConfiguration());
    }

    /**
     * Get the Titan/Hadoop (Faunus) configuration
     */
    public ModifiableHadoopConfiguration getFaunusConfiguration() {
        return faunusConf;
    }

    /**
     * Get the Titan configuration
     */
    public SerializableBaseConfiguration getTitanConfiguration() {
        return titanConfig;
    }


    /**
     * Get Titan input format class name
     */
    public String getInputFormatClassName() {
        String titanVersion = faunusConf.get(TITAN_INPUT_VERSION);
        String inputFormatClassName = SETUP_PACKAGE_PREFIX +
                titanVersion + SETUP_CLASS_NAME;
        return(inputFormatClassName);
    }

    /**
     * Compute the hashcode based on the Titan property values.
     * <p/>
     * The hashcode allows us to compare two configuration objects with the same Titan property entries.
     *
     * @return Hashcode based on property values
     */
    @Override
    public int hashCode() {
        return (this.titanConfig.hashCode());
    }

    /**
     * Configuration objects are considered equal if they contain the same Titan property keys and values.
     *
     * @param obj Configuration object to compare
     * @return True if the configuration objects have matching Titan property keys and values
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        TitanHadoopCacheConfiguration that = (TitanHadoopCacheConfiguration) obj;
        return (this.titanConfig.equals(that.getTitanConfiguration()));
    }

    /**
     * Create Titan configuration from Hadoop configuration
     *
     * @param hadoopConfig Hadoop configuration
     * @return Titan configuration
     */
    private SerializableBaseConfiguration createTitanConfiguration(Configuration hadoopConfig) {
        SerializableBaseConfiguration titanConfig = new SerializableBaseConfiguration();
        Iterator<Map.Entry<String, String>> itty = hadoopConfig.iterator();

        while (itty.hasNext()) {
            Map.Entry<String, String> entry = itty.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith(TITAN_HADOOP_PREFIX)) {
                titanConfig.setProperty(key.substring(TITAN_HADOOP_PREFIX.length() + 1), value);
            }
        }
        return (titanConfig);
    }
}
