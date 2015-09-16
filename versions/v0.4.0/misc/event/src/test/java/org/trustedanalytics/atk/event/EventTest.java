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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

@RunWith(JUnit4.class)
public class EventTest {

    @Before
    public void setup() {
        EventContext.setCurrent(null);
    }

    @After
    public void tearDown() {
        EventContext.setCurrent(null);
    }

    private EventData eventData(Enum message) {
        return new EventData(Severity.INFO, null, null, null, 0, message.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void events_require_instant() {
        EventContext ctx = new EventContext("ctx");
        EventData data = eventData(EventTestMessages.SOMETHING_HAPPENED);
        new Event(ctx, null, data);
    }

    @Test(expected = IllegalArgumentException.class)
    public void events_require_data() {
        EventContext ctx = new EventContext("ctx");
        new Event(ctx, new Instant(), null);
    }

    @Test
    public void events_have_unique_Ids() {
        Event e1 = new Event(null, new Instant(), eventData(EventTestMessages.SOMETHING_HAPPENED));
        Event e2 = new Event(null, new Instant(), eventData(EventTestMessages.SOMETHING_HAPPENED));

        assertThat(e1.getId(), is(notNullValue()));
        assertThat(e1.getId(), is(not(equalTo(e2.getId()))));
    }


    @Test
    public void events_with_contexts_inherit_correlation_id_from_context() {
        EventContext ctx = new EventContext("ctx");
        Event e1 = new Event(ctx, new Instant(), eventData(EventTestMessages.SOMETHING_HAPPENED));

        assertThat(e1.getId(), is(not(equalTo(ctx.getCorrelationId()))));
        assertThat(e1.getCorrelationId(), equalTo(ctx.getCorrelationId()));
    }

    @Test
    public void events_without_contexts_use_their_ids_as_the_correlation_id() {
        EventContext.setCurrent(null);
        Event e1 = new Event(null, new Instant(), eventData(EventTestMessages.SOMETHING_HAPPENED));

        assertThat(e1.getId(), equalTo(e1.getCorrelationId()));
    }

    @Test
    public void events_can_carry_additional_string_data() {
        Event e = EventContext.event(EventTestMessages.SOMETHING_HAPPENED)
                .put("hello", "world")
                .build();

        assertThat(e.getData().get("hello"), is(equalTo("world")));
    }

    @Test
    public void events_can_use_literal_strings_and_error_codes() {
        Event e = EventContext.event(Severity.INFO, 150, "Something happened!")
                .put("hello", "world")
                .build();

        assertThat(e.getMessage(), is(equalTo("Something happened!")));
        assertThat(e.getMessageCode(), is(equalTo(150)));
        assertThat(e.getData().get("hello"), is(equalTo("world")));

    }

    @Test
    public void events_inherit_context_data_from_most_recent_context() {
        EventContext context1 = new EventContext("ctx1");
        context1.put("hello", "world");
        EventContext context2 = new EventContext("ctx2");
        context2.put("hello", "galaxy");

        //Just to prove that contexts aren't sharing context data
        assertThat(context1.get("hello"), is(not(equalTo(context2.get("hello")))));

        Event event = new Event(context2, new Instant(), eventData(EventTestMessages.SOMETHING_HAPPENED));

        assertThat(event.getData().get("hello"), is(equalTo("galaxy")));
    }

    @Test
    public void events_inherit_context_data_across_threads() throws InterruptedException {
        EventContext context1 = new EventContext("ctx1");
        context1.put("hello", "world");
        EventContext context2 = new EventContext("ctx2");
        context2.put("hello", "galaxy");

        //Just to prove that contexts aren't sharing context data
        assertThat(context1.get("hello"), is(not(equalTo(context2.get("hello")))));

        final HashMap<String,String> data = new HashMap<>();

        Thread thread = new Thread(new Runnable() {

            @Override
            public void run() {
                Event event = EventContext.event(EventTestMessages.SOMETHING_HAPPENED).build();
                data.putAll(event.getData());
            }
        });
        thread.start();
        thread.join();

        assertThat(data.get("hello"), is(equalTo("galaxy")));
    }

    static enum EventTestMessages {
        SOMETHING_HAPPENED
    }

    @Ignore
    @Test
    public void Event_toString_includes_all_relevant_event_state() {
        EventContext ctx = new EventContext("Ctx");
        EventContext ctx2 = new EventContext("Ctx2");
        ctx.put("ctxdata", "test");
        ctx2.put("ctxdata", "another test");
        Exception e = new RuntimeException("oops");
        Event event = EventContext.event(EventTestMessages.SOMETHING_HAPPENED, "good", "bad")
                        .put("hello", "world")
                        .addMarker("NOTIFY_ADMIN")
                        .addException(e)
                        .build();
        ctx2.close();
        ctx.close();

        String s = event.toString();
        System.out.println(s);
        JSONObject json = (JSONObject) JSONValue.parse(s);
        assertThat("Failed to parse json", json, not(nullValue()));
        assertThat((String)json.get("id"), equalTo(event.getId()));
        assertThat((String)json.get("severity"), equalTo(event.getSeverity().toString()));
        assertThat(((Long)json.get("messageCode")).intValue(), equalTo(event.getMessageCode()));
        assertThat((String)json.get("message"), equalTo(event.getMessage()));
        JSONArray substitutions = (JSONArray) json.get("substitutions");
        String[] subs = new String[substitutions.size()];
        for(int i = 0; i < subs.length; i++) {
            subs[i] = (String) substitutions.get(i);
        }
        assertThat(subs, equalTo(event.getSubstitutions()));
        JSONArray markers = (JSONArray) json.get("markers");
        String[] marks = new String[markers.size()];
        for(int i = 0; i < marks.length; i++) {
            marks[i] = (String)markers.get(i);
        }
        JSONArray errors = (JSONArray)json.get("errors");
        String[] errs = new String[errors.size()];
        for(int i = 0; i < errs.length; i++) {
            errs[i] = (String)errors.get(i);
        }
        assertThat((String)json.get("corId"), equalTo(event.getCorrelationId()));
        assertThat((String)json.get("directory"), equalTo(event.getWorkingDirectory()));
        assertThat((String)json.get("process"), equalTo(event.getProcessId()));
        assertThat((String)((JSONObject)json.get("data")).get("hello"), equalTo("world"));
        JSONArray contexts = (JSONArray) json.get("contexts");
        JSONObject ctxJson = (JSONObject) contexts.get(0);
        JSONObject ctxJson2 = (JSONObject) contexts.get(1);
        assertThat((String)((JSONObject)ctxJson.get("data")).get("ctxdata"), equalTo("another test"));
        assertThat((String)((JSONObject)ctxJson2.get("data")).get("ctxdata"), equalTo("test"));
        assertThat(marks, is(equalTo(new String[]{"NOTIFY_ADMIN"})));
        assertThat(errs[0], is(equalTo(e.toString())));

    }
}
