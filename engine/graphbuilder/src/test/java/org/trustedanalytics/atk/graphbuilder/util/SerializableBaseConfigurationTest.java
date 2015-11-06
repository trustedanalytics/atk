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


package org.trustedanalytics.atk.graphbuilder.util;

import org.junit.Test;

import static junit.framework.Assert.*;


public class SerializableBaseConfigurationTest {

    @Test
    public void testEquality() throws Exception {
        SerializableBaseConfiguration config1 = new SerializableBaseConfiguration();
        config1.setProperty("key1", "value1");
        config1.setProperty("key2", 56);
        config1.setProperty("key3", false);

        SerializableBaseConfiguration config2 = new SerializableBaseConfiguration();
        config2.setProperty("key1", "value1");
        config2.setProperty("key2", 56);
        config2.setProperty("key3", false);

        assertTrue(config1.equals(config2));
        assertEquals(config1.hashCode(), config2.hashCode());
    }

    @Test
    public void testInEquality() throws Exception {
        SerializableBaseConfiguration config1 = new SerializableBaseConfiguration();
        config1.setProperty("key1", "value1");
        config1.setProperty("key2", 56);
        config1.setProperty("key3", false);

        SerializableBaseConfiguration config2 = new SerializableBaseConfiguration();
        config2.setProperty("key1", "notequal");
        config2.setProperty("key2", 56);
        config2.setProperty("key3", false);

        assertFalse(config1.equals(config2));
        assertNotSame(config1.hashCode(), config2.hashCode());

        assertFalse(config1.equals(null));

        assertFalse(config2.equals(new String("test")));
    }

}
