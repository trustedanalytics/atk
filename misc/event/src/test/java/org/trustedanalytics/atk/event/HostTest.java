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

package org.trustedanalytics.atk.event;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class HostTest {

    @RunWith(PowerMockRunner.class)
    @PrepareForTest(Host.class)
    public static class MockedInet {
        @Test
        public void Host_provides_default_hostname_when_java_cant_determine_hostname() throws UnknownHostException {
            PowerMockito.mockStatic(InetAddress.class);

            PowerMockito.when(InetAddress.getLocalHost()).thenThrow(new UnknownHostException());

            assertThat(Host.getMachineName(),
                    is(equalTo("<UNKNOWN>")));
        }
    }

    @Test @Ignore("Doesn't work on build server for some reason")
    public void Host_captures_hostname() throws UnknownHostException {

        PowerMockito.mockStatic(InetAddress.class);

        PowerMockito.when(InetAddress.getLocalHost()).thenCallRealMethod();

        assertThat(Host.getMachineName(),
                is(equalTo(InetAddress.getLocalHost().getHostName())));
    }



    @Test
    public void Host_constructor_is_private() throws IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
        Constructor constructor = Host.class.getDeclaredConstructor();
        Assert.assertThat(Modifier.isPrivate(constructor.getModifiers()), is(true));
        constructor.setAccessible(true);
        constructor.newInstance();
    }
}
