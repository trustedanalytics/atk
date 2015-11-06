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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import java.util.Iterator;
import java.util.Map;

import static org.trustedanalytics.atk.giraph.io.titan.common.GiraphTitanConstants.GIRAPH_TITAN;

/**
 * Converts a Giraph configuration file to a Titan configuration file. For all
 * Titan specific properties, the conversion removes the Giraph prefix and
 * provides to Titan's graph factory.
 */
public class GiraphToTitanGraphFactory {
    /**
     * prevent instantiation of utilize class
     */
    private GiraphToTitanGraphFactory() {
    }

    /**
     * generateTitanConfiguration from Giraph configuration
     *
     * @param hadoopConfig : Giraph configuration
     * @param prefix : prefix to remove for Titan
     * @return BaseConfiguration
     */
    public static BaseConfiguration createTitanBaseConfiguration(Configuration hadoopConfig, String prefix) {

        BaseConfiguration titanConfig = new BaseConfiguration();
        Iterator<Map.Entry<String, String>> itty = hadoopConfig.iterator();

        while (itty.hasNext()) {
            Map.Entry<String, String> entry = itty.next();
            String key = entry.getKey();
            String value = entry.getValue();

            if (key.startsWith(prefix)) {
                titanConfig.setProperty(key.substring(prefix.length() + 1), value);
            }
        }
        return titanConfig;
    }


    public static void addFaunusInputConfiguration(Configuration hadoopConfig) {
        BaseConfiguration titanConfig = createTitanBaseConfiguration(hadoopConfig, GIRAPH_TITAN.get(hadoopConfig));
        Iterator keys = titanConfig.getKeys();
        String prefix = "titan.hadoop.input.conf.";
        while (keys.hasNext())
        {
            String key = (String) keys.next();
            hadoopConfig.set(prefix + key, titanConfig.getString(key));
        }
    }
}
