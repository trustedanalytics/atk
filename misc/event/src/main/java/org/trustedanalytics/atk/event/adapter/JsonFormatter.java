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
import org.trustedanalytics.atk.event.EventContext;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * Formats events using JSON
 */
public class JsonFormatter {

    private static final DateFormat ISO_8601_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");

    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        ISO_8601_FORMAT.setTimeZone(tz);
    }

    @SuppressWarnings("unchecked")
    public static JSONObject toJson(EventContext ec) {
        JSONObject json = new JSONObject();
        json.put("name", ec.getName());
        json.put("data", ec.getData());
        return json;
    }

    @SuppressWarnings("unchecked")
    public static JSONObject toJson(Event e) {
        List<JSONObject> contexts = new ArrayList<>();
        EventContext current = e.getContext();
        while (current != null) {
            contexts.add(toJson(current));
            current = current.getParent();
        }
        JSONObject json = new JSONObject();
        json.put("id", e.getId());
        json.put("corId", e.getCorrelationId());
        json.put("severity", e.getSeverity().name());
        json.put("messageCode", e.getMessageCode());
        json.put("message", e.getMessage());
        json.put("machine", e.getMachine());
        json.put("user", e.getUser());
        json.put("threadId", e.getThreadId());
        json.put("threadName", e.getThreadName());
        json.put("date", ISO_8601_FORMAT.format(e.getDate()));
        json.put("substitutions", toJsonArray(e.getSubstitutions()));
        json.put("markers", toJsonArray(e.getMarkers()));
        json.put("errors", toJsonArray(e.getErrors()));
        json.put("contexts", contexts);
        json.put("data", e.getData());
        json.put("directory", e.getWorkingDirectory());
        json.put("process", e.getProcessId());
        return json;
    }

    private static JSONArray toJsonArray(Object[] array) {
        JSONArray jsonArray = new JSONArray();
        for (Object anArray : array) {
            if (anArray instanceof Throwable) {
                jsonArray.add(ExceptionUtils.getStackTrace((Throwable) anArray));

            }
            //noinspection unchecked
            jsonArray.add(String.valueOf(anArray));
        }
        return jsonArray;
    }

}
