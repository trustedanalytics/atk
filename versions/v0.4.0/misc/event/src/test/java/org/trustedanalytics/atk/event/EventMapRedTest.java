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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

@RunWith(PowerMockRunner.class)
public class EventMapRedTest {

    @After
    public void tearDown() {
        EventContext.setCurrent(null);
    }

    static enum EventMapRedTestMessages {
        SOMETHING_HAPPENED
    }
    public static class StoreAnEventMapper
            implements Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void map(IntWritable key, IntWritable value, OutputCollector<IntWritable, IntWritable> intWritableIntWritableOutputCollector, Reporter reporter) throws IOException {
            EventMapRedTest.capturedEvent = EventContext.event(EventMapRedTestMessages.SOMETHING_HAPPENED).build();
            intWritableIntWritableOutputCollector.collect(value, key);
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void configure(JobConf entries) {
            String flatContext = entries.get("__event_ctx");
            if (flatContext != null) {
                EventContext context = EventContext.deserialize(flatContext);
            }
        }
    }

    public static Event capturedEvent;
    MapDriver<IntWritable,IntWritable,IntWritable,IntWritable> driver;

    @Before
    public void setup() {
        StoreAnEventMapper mapper = new StoreAnEventMapper();
        driver = new MapDriver();
        driver.setMapper(mapper);
        EventMapRedTest.capturedEvent = null;
        EventContext.setCurrent(null);
    }

    @Test
    public void mapper_captures_event() throws IOException {

        driver.withInput(new IntWritable(1), new IntWritable(2));
        driver.withOutput(new IntWritable(2), new IntWritable(1));
        driver.runTest();

        assertThat(EventMapRedTest.capturedEvent, is(notNullValue()));
    }

    @Test
    public void mapper_event_includes_callers_context() throws IOException {

        EventContext ctx = new EventContext("Ctx");
        driver.withInput(new IntWritable(1), new IntWritable(2));
        driver.withOutput(new IntWritable(2), new IntWritable(1));
        driver.runTest();

        assertThat(EventMapRedTest.capturedEvent.getContextNames(),
                is(equalTo(new String[]{"Ctx"})));
    }

    @Test
    public void mapper_event_includes_callers_context_on_another_thread() throws InterruptedException {

        EventContext ctx = new EventContext("Ctx");
        driver.withInput(new IntWritable(1), new IntWritable(2));
        driver.withOutput(new IntWritable(2), new IntWritable(1));
        final MapDriver<IntWritable,IntWritable,IntWritable,IntWritable> driv = driver;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    driv.runTest();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        EventContext ctx2 = new EventContext("Other");

        thread.join();
        assertThat(EventMapRedTest.capturedEvent.getContextNames(),
                is(equalTo(new String[]{"Ctx"})));
    }

    @Test
    public void mapper_event_includes_callers_context_on_another_thread_serialized() throws InterruptedException {

        EventContext ctx = new EventContext("Ctx");
        ctx.put("test", "passed");
        String flatCtx = EventContext.serialize(ctx);
        EventContext.setCurrent(null);
        driver.withInput(new IntWritable(1), new IntWritable(2));
        driver.withOutput(new IntWritable(2), new IntWritable(1));
        Configuration configuration = new Configuration();
        configuration.set("__event_ctx", flatCtx);
        driver.withConfiguration(configuration);
        final MapDriver<IntWritable,IntWritable,IntWritable,IntWritable> driv = driver;

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    driv.runTest();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();

        thread.join();

        assertThat(EventMapRedTest.capturedEvent.getContextNames(),
                is(equalTo(new String[]{"Ctx"})));
        assertThat(EventMapRedTest.capturedEvent.getData().get("test"),
                is(equalTo("passed")));
    }
}
