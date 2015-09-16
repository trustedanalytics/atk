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

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates event-specific data
 */
class EventData {

    private final Severity severity;
    private final Throwable[] errors;
    private final Map<String, String> data;
    private final String[] markers;
    private final int messageCode;
    private final String message;
    private final String[] substitutions;

    /**
     * Parameters are stored for later use by Event, with the obvious name-based mapping to Event properties.
     *
     * @see Event
     */
    EventData(
            Severity severity,
            Throwable[] errors,
            Map<String, String> data,
            String[] markers,
            int messageCode,
            String message,
            String... substitutions) {
        if (severity == null) {
            throw new IllegalArgumentException("Severity cannot be null");
        }
        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }
        this.severity = severity;
        this.errors = errors == null ? new Throwable[0] : errors;
        this.data = data == null ? new HashMap<String, String>() : data;
        this.markers = markers == null ? new String[0] : markers;
        this.messageCode = messageCode;
        String formatted;
        try {
            formatted = MessageFormat.format(message, (Object[])substitutions);
        } catch (Exception e) {
            formatted = message;
            this.data.put("eventdata_formatting_exception", e.getMessage());
        }
        this.message = formatted;
        this.substitutions = substitutions;
    }

    public Severity getSeverity() {
        return severity;
    }

    public Map<String, String> getData() {
        return data;
    }

    public String getMessage() {
        return message;
    }

    public int getMessageCode() { return messageCode; }

    public String[] getSubstitutions() {
        return substitutions;
    }

    public String[] getMarkers() {
        return markers;
    }

    public Throwable[] getErrors() {
        return errors;
    }



}
