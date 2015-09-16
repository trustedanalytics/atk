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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(JUnit4.class)
public class EventContextTest {

    @Before
    public void setup() {
        EventContext.setCurrent(null);
    }

    @After
    public void tearDown() {
        EventContext.setCurrent(null);
    }

    @Test
    public void event_contexts_have_names() {
        EventContext context = new EventContext("Context1");
        assertThat(context.getName(), equalTo("Context1"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void event_contexts_null_throws_exception() {
        EventContext context = new EventContext(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void dehydrate_null_throws_exception() {
        EventContext.serialize(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void readFields_null_throws_exception() throws IOException {
        EventContext context = new EventContext("ctx");
        context.readFields(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void write_null_throws_exception() throws IOException {
        EventContext context = new EventContext("ctx");
        context.write(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void rehydrate_null_throws_exception() {
        EventContext.deserialize(null);
    }
    @Test
    public void event_contexts_can_carry_additional_string_data() {
        EventContext context = new EventContext("ctx");

        context.put("hello", "world");

        assertThat(context.get("hello"), is(equalTo("world")));
    }

    @Test
    public void event_context_cleared_after_close() {
        try (EventContext ctx = EventContext.enter("ctx")) {
            assertThat(EventContext.getCurrent(), is(ctx));
        }
        assertThat(EventContext.getCurrent(), is(nullValue()));
    }

    @Test(expected = RuntimeException.class)
    public void event_context_dehydrate_wraps_IOException_in_runtime_exceptions() throws IOException {
        EventContext mockContext = mock(EventContext.class);
        doThrow(new IOException("oops"))
                .when(mockContext).write(any(DataOutput.class));
        EventContext.serialize(mockContext);
    }


    @Test(expected = RuntimeException.class)
    public void event_context_rehydrate_wraps_IOException_in_runtime_exceptions() throws IOException {
        EventContext mockContext = mock(EventContext.class);
        doThrow(new IOException("oops"))
                .when(mockContext).readFields(any(DataInput.class));
        EventContext.deserialize("foo");
    }

    @Test
    public void first_event_context_creates_correlation_ID() {
        EventContext context = new EventContext("ctx");
        String corId = context.getCorrelationId();
        assertThat(corId, is(notNullValue()));
    }

    @Test
    public void second_event_context_inherits_correlation_ID_from_first() {
        EventContext context = new EventContext("ctx");
        String corId = context.getCorrelationId();
        EventContext context2 = new EventContext("test");
        assertThat(context2.getCorrelationId(), is(equalTo(corId)));
    }

    @Test
    public void correlation_ids_are_unique() {
        EventContext context = new EventContext("ctx");
        String corId = context.getCorrelationId();
        context.close();
        EventContext context2 = new EventContext("test");
        assertThat(context2.getCorrelationId(), is(not(equalTo(corId))));
    }
}
