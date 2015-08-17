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

import org.trustedanalytics.atk.event.adapter.SLF4JLogAdapter;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import static org.trustedanalytics.atk.event.RegularExpressionMatcher.matchesPattern;
import static org.junit.Assert.assertThat;

@RunWith(Theories.class)
public class SLF4JLogAdapterTest {
    PrintStream printStream;

    ByteArrayOutputStream byteStream;

    static enum Msg {
        SOMETHING_HAPPENED
    }

    @Before
    public void before() {
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "DEBUG, A1");
        properties.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
        // Print the date in ISO 8601 format
        properties.setProperty("log4j.appender.A1.layout.ConversionPattern", "%d [%t] %-5p %c - %X{extra} %m%n");
        properties.setProperty("log4j.logger.com.foo", "WARN");
        printStream = System.out;
        byteStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(byteStream));
        EventLogger.setImplementation(new SLF4JLogAdapter());
        PropertyConfigurator.configure(properties);
    }

    @After
    public void after() {
        System.setOut(printStream);
    }

    @DataPoints
    public static Severity[] data() {
        return new Severity[] {
                Severity.DEBUG,
                Severity.ERROR,
                Severity.FATAL,
                Severity.INFO,
                Severity.TRACE,
                Severity.WARN
        };
    }

    @Theory @Ignore("Needs work to support sbt-based testing")
    public void honors_log4j_formatting(Severity severity) throws UnsupportedEncodingException {
        System.out.flush();
        try(EventContext ctx = EventContext.enter("Ctx")) {
            ctx.put("extra", "data");
            EventLogger.log(EventContext.event(severity, Msg.SOMETHING_HAPPENED, "reasonably good")
                    .addMarker("HELLO")
                    .addMarker("WORLD")
                    .build());

        }
        System.out.flush();
        String output = new String(byteStream.toByteArray(), "UTF-8").trim();
        //FATAL isn't a supported log level in log4j, so rendered as ERROR.
        String severityString = severity == Severity.FATAL ? "ERROR" : severity.toString();
        if (severityString.length() < 5) {
            for(int i = severityString.length(); i < 5; i++) {
                severityString += " ";
            }
        }

        String pattern = "^[0-9\\-.:]+ \\[[^\\]]+\\] "
                + severityString + " Ctx - data \\|com\\.trustedanalytics\\.event\\.SLF4JLogAdapterTest" +
                "\\$Msg\\.SOMETHING_HAPPENED\\|0\\[reasonably good\\]:\\[HELLO, WORLD\\]$";

        //Trace is lower than DEBUG, so trace messages shouldn't show at all based on the
        //config we used.
        if (severity == Severity.TRACE)
            pattern = "";

        assertThat(output, matchesPattern(pattern));
    }
}
