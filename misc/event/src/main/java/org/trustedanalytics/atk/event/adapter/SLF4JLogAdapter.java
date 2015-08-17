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

package org.trustedanalytics.atk.event.adapter;

import org.trustedanalytics.atk.event.Event;
import org.trustedanalytics.atk.event.EventLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * EventLog implementation that sends events through SLF4J.
 */
public class SLF4JLogAdapter implements EventLog {

    private String join(String separator, String[] parts) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            builder.append(parts[i]);
            if (i + 1 != parts.length) {
                builder.append(separator);
            }
        }
        return builder.toString();
    }

    @Override
    public void log(Event e) {
        Logger factory = LoggerFactory.getLogger(join(":", e.getContextNames()));
        MDC.setContextMap(e.getData());
        String[] markers = e.getMarkers();
        Marker marker = null;
        if (markers.length > 0) {
            marker = MarkerFactory.getDetachedMarker(markers[0]);
            for (int i = 1; i < markers.length; i++) {
                marker.add(MarkerFactory.getMarker(markers[i]));
            }
        }
        String logMessage = getLogMessage(e);
        Throwable[] errors = e.getErrors();
        Throwable firstError = errors.length > 0 ? errors[0] : null;
        switch (e.getSeverity()) {
        case INFO:
            factory.info(marker, logMessage, firstError);
            break;
        case WARN:
            factory.warn(marker, logMessage, firstError);
            break;
        case ERROR:
            factory.error(marker, logMessage, firstError);
            break;
        case DEBUG:
            factory.debug(marker, logMessage, firstError);
            break;
        case TRACE:
            factory.trace(marker, logMessage, firstError);
            break;
        case FATAL:
            factory.error(marker, logMessage, firstError);
            break;
        default:
            throw new RuntimeException("Unrecognized severity level: " + e.getSeverity().toString());
        }
    }

    private String getLogMessage(Event e) {
        StringBuilder builder = new StringBuilder("|");
        builder.append(e.getMessage());
        builder.append('|');
        builder.append(e.getMessageCode());
        builder.append("[");
        boolean first = true;
        for (String sub : e.getSubstitutions()) {
            if (!first) {
                builder.append(':');
            }
            builder.append(sub);
            first = false;
        }
        builder.append("]:[");
        first = true;
        for (String marker : e.getMarkers()) {
            if (!first) {
                builder.append(", ");
            }
            builder.append(marker);
            first = false;
        }
        builder.append(']');
        return builder.toString();
    }
}
