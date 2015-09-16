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

/**
 * EventLogger logs events to log files, message queues, or other destinations based on
 * configuration, in a manner similar to log4j, and in fact log4j is one possible destination
 * for EventLogger log messages.
 * <p/>
 * Unlike systems such as log4j, the EventLogger supports rich event data including nested
 * execution contexts, and delayed translation support, such that translation of log
 * entries can be done long after the message has been logged. This supports scenarios where
 * there are several languages in use among the people who wish to view the logs.
 *
 * @see EventContext
 * @see Event
 */
public class EventLogger {

    private static EventLog EVENT_LOG;

    private EventLogger() {
    }

    /**
     * Logs an event
     *
     * @param event the event to log
     */
    public static void log(Event event) {
        if (EVENT_LOG == null) {
            System.err.println("Event log not configured, please set the event logger before logging.");
        } else {
            EVENT_LOG.log(event);
        }
    }

    /**
     * Logs an event at TRACE severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void trace(Enum message, String... substitutions) {
        log(EventContext.event(Severity.TRACE,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at DEBUG severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void debug(Enum message, String... substitutions) {
        log(EventContext.event(Severity.DEBUG,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at INFO severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void info(Enum message, String... substitutions) {
        log(EventContext.event(Severity.INFO,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at WARN severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void warn(Enum message, String... substitutions) {
        log(EventContext.event(Severity.WARN,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at ERROR severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void error(Enum message, String... substitutions) {
        log(EventContext.event(Severity.ERROR,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at ERROR severity
     *
     * @param message       the message to log
     * @param error the error to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void error(Enum message, Throwable error, String... substitutions) {
        log(EventContext.event(Severity.ERROR,
                message,
                substitutions)
                .addException(error)
                .build());
    }

    /**
     * Logs an event at FATAL severity
     *
     * @param message       the message to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void fatal(Enum message, String... substitutions) {
        log(EventContext.event(Severity.FATAL,
                message,
                substitutions).build());
    }

    /**
     * Logs an event at FATAL severity
     *
     * @param message       the message to log
     * @param error the error to log
     * @param substitutions string substitutions to replace placeholders in the message string
     */
    public static void fatal(Enum message, Throwable error, String... substitutions) {
        log(EventContext.event(Severity.FATAL,
                message,
                substitutions)
                .addException(error)
                .build());
    }

    /**
     * Change the logging implementation. Generally this should be called once during application
     * startup.
     *
     * @param eventLog the logging implementation to use
     */
    public static void setImplementation(EventLog eventLog) {   EventLogger.EVENT_LOG = eventLog; }

    /**
     * Check if we have a logging implementation. If you desire not to keep setting it after it's been set once.
     *
     * @return the current event logger null if none is set
     */
    public static EventLog getImplementation() { return EventLogger.EVENT_LOG; }
}
