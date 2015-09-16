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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder class for Events. Use this class to add additional information
 * to your events.
 */
public class EventBuilder {
    private final Severity severity;
    private final String message;
    private final String[] substitutions;
    private final EventContext context;
    private final Instant instant;
    private final List<String> markers = new ArrayList<>();
    private final Map<String, String> data = new HashMap<>();
    private final List<Throwable> errors = new ArrayList<>();
    private final int messageCode;

    /**
     * Constructor that takes all the data that are required in order
     * to construct a minimal Event.
     *
     * @param context the event context
     * @param severity the severity level
     * @param message the message key
     * @param substitutions the string substitutions for the message
     * @see Event
     * @throws IllegalArgumentException if severity or message are null
     */
    public EventBuilder(EventContext context,
                        Severity severity,
                        Enum message,
                        String... substitutions) {
        if (severity == null) {
            throw new IllegalArgumentException("Severity cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        this.context = context;
        this.severity = severity;
        this.messageCode = 0;
        this.message = formatDefaultLogMessage(message);
        this.substitutions = substitutions;
        this.instant = new Instant();
    }

    /**
     * Constructor that takes all the data that are required in order
     * to construct a minimal Event.
     *
     * @param context the event context
     * @param severity the severity level
     * @param messageCode a numeric identifier for the message
     * @param message the literal message
     * @param substitutions the string substitutions for the message
     * @see Event
     * @throws IllegalArgumentException if severity or message are null
     */
    public EventBuilder(EventContext context,
                        Severity severity,
                        int messageCode,
                        String message,
                        String... substitutions) {
        if (severity == null) {
            throw new IllegalArgumentException("Severity cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        this.context = context;
        this.severity = severity;
        this.messageCode = messageCode;
        this.message = message;
        this.substitutions = substitutions;
        this.instant = new Instant();
    }
    /**
     * Add additional data to this event
     *
     * @param key the name of the additional data
     * @param value the value of the additional data
     * @return an updated EventBuilder for further customization
     */
    public EventBuilder put(String key, String value) {
        data.put(key, value);
        return this;
    }

    /**
     * Add a marker (tag) to this event. Markers facilitate more flexible reporting on events.
     *
     * @param marker the marker to add to the event
     * @return an updated EventBuilder for further customization
     */
    public EventBuilder addMarker(String marker) {
        markers.add(marker);
        return this;
    }

    /**
     * Add a Throwable associated with this event
     *
     * @param e the throwable to associate
     * @return an updated EventBuilder for further customization
     */
    public EventBuilder addException(Throwable e) {
        errors.add(e);
        return this;
    }

    /**
     * Generate a read-only Event based on the parameters configured
     * by the builder.
     * @return the finished Event, which can then be passed to {@link EventLog#log}
     */
    public Event build() {
        String[] marks = this.markers.toArray(new String[this.markers.size()]);
        Throwable[] throwables = this.errors.toArray(new Throwable[this.errors.size()]);
        return new Event(context, instant,
                new EventData(severity, throwables, data, marks,
                                messageCode, message, substitutions));
    }

    /**
     * Generates a log message for the given Enum
     */
    static String formatDefaultLogMessage(Enum e) {
        String cls = e.getClass().getName();
        StringBuilder builder = new StringBuilder(cls);
        builder.append('.');
        builder.append(e.toString());
        return builder.toString();
    }
}
