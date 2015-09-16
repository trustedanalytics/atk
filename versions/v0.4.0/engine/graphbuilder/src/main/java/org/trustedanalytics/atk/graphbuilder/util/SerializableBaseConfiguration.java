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

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trustedanalytics.atk.graphbuilder.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.configuration.AbstractConfiguration;

/**
 * An implementation that implements Serializable that was
 * otherwise copied from Apache's BaseConfiguration.
 *
 * This implementation also overrides the hashcode and equality methods to enable
 * comparison of configuration objects based on their key values.
 */
public class SerializableBaseConfiguration extends AbstractConfiguration implements Serializable {

    /** stores the configuration key-value pairs */
    private Map store = new LinkedMap();

    /**
     * Adds a key/value pair to the map. This routine does no magic morphing. It ensures the keylist is maintained
     * 
     * @param key key to use for mapping
     * @param value object to store
     */
    protected void addPropertyDirect(String key, Object value) {
        Object previousValue = getProperty(key);

        if (previousValue == null) {
            store.put(key, value);
        } else if (previousValue instanceof List) {
            // the value is added to the existing list
            ((List) previousValue).add(value);
        } else {
            // the previous value is replaced by a list containing the previous value and the new value
            List list = new ArrayList();
            list.add(previousValue);
            list.add(value);

            store.put(key, list);
        }
    }

    /**
     * Read property from underlying map.
     * 
     * @param key key to use for mapping
     * 
     * @return object associated with the given configuration key.
     */
    public Object getProperty(String key) {
        return store.get(key);
    }

    /**
     * Check if the configuration is empty
     * 
     * @return <code>true</code> if Configuration is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty() {
        return store.isEmpty();
    }

    /**
     * check if the configuration contains the key
     * 
     * @param key the configuration key
     * 
     * @return <code>true</code> if Configuration contain given key, <code>false</code> otherwise.
     */
    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    /**
     * Clear a property in the configuration.
     * 
     * @param key the key to remove along with corresponding value.
     */
    protected void clearPropertyDirect(String key) {
        if (containsKey(key)) {
            store.remove(key);
        }
    }

    public void clear() {
        fireEvent(EVENT_CLEAR, null, null, true);
        store.clear();
        fireEvent(EVENT_CLEAR, null, null, false);
    }

    /**
     * Get the list of the keys contained in the configuration repository.
     * 
     * @return An Iterator.
     */
    public Iterator getKeys() {
        return store.keySet().iterator();
    }

    /**
     * Compute the hashcode based on the property values.
     *
     * The hashcode allows us to compare two configuration objects with the same property entries.
     *
     * @return Hashcode based on property values
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        for (Object property : store.keySet()) {
            String value = store.get(property).toString();
            if (value != null) {
                result = prime * result + value.hashCode();
            }
        }
        return result;
    }

    /**
     * Configuration objects are considered equal if they contain the same property keys and values.
     *
     * @param obj Configuration object to compare
     * @return True if the configuration objects have matching property keys and values
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        SerializableBaseConfiguration that = (SerializableBaseConfiguration) obj;
        for (Object property : store.keySet()) {
            String thisValue = this.store.get(property).toString();
            String thatValue = that.store.get(property).toString();

            if (thisValue == thatValue) {
                continue;
            }
            if (thisValue == null || !thisValue.equals(thatValue)) {
                return false;
            }
        }

        return true;
    }


}
