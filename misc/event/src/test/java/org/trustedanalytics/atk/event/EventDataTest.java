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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class EventDataTest {

    @Test(expected = IllegalArgumentException.class)
    public void EventData_requires_severity() {

        new EventData(null, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString());

    }

    @Test(expected = IllegalArgumentException.class)
    public void EventData_requires_message() {

        new EventData(Severity.INFO, null, null, null, 0, null, (String[])null);

    }

    public void EventData_defaults_empty_array_for_throwable() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                        .getErrors(),
                not(nullValue()));

    }

    public void EventData_defaults_empty_map_for_data() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                    .getData(),
                not(nullValue()));

    }

    public void EventData_defaults_empty_array_for_markers() {

        assertThat(new EventData(Severity.INFO, null, null, null, 0, EventLoggerTest.Msg.SOMETHING_HAPPENED.toString())
                        .getMarkers(),
                not(nullValue()));

    }


}
