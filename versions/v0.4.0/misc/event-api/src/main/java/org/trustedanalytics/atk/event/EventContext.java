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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EventContext provides context for events that are created while a
 * given event context is active. Contexts are thread-specific, and are inherited
 * by threads that the current thread creates.
 */
public class EventContext implements AutoCloseable {

    private static final InheritableThreadLocal<EventContext> CURRENT = new InheritableThreadLocal<>();
    private String name;
    private final ConcurrentHashMap<String, String> context = new ConcurrentHashMap<>();
    private final EventContext parent;
    private final String correlationId;

    /**
     * Creates an event context. If another context was already active, that context
     * will be considered the parent context of this context.
     *
     * @param name the name of the context as it should appear in logs
     */
    EventContext(String name) {
        if (name == null || name == "") {
            throw new IllegalArgumentException("Event context name cannot be null or empty");
        }
        this.name = name;
        parent = EventContext.getCurrent();
        if (parent != null) {
            correlationId = parent.getCorrelationId();
        } else {
            correlationId = UUID.randomUUID().toString();
        }
        EventContext.setCurrent(this);

    }

    /**
     * Gets the event context for the current thread.
     * @return the current event context, or null if no context has been established
     */
    public static EventContext getCurrent() {
        return CURRENT.get();
    }

    /**
     * Sets the current event context.
     *
     * @param current the context to set as the current context for this thread
     */
    public static void setCurrent(EventContext current) {
        EventContext.CURRENT.set(current);
    }

    /**
     * The name of the context, which should be short but descriptive to
     * assist in troubleshooting and log analysis.
     *
     */
    public String getName() {
        return name;
    }

    /**
     * Adds data to the context
     */
    public void put(String key, String value) {
        context.put(key, value);
    }

    /**
     * Retrieves data from the context.
     */
    public String get(String key) {
        return context.get(key);
    }

    /**
     * Retrieves a map of all the data associated with the context
     */
    public Map<? extends String, ? extends String> getData() {
        HashMap<String, String> map = new HashMap<>();
        map.putAll(context);
        return map;
    }

    /**
     * Serializes the event context (and all its parents) to the given DataOutput
     */
    public void write(DataOutput dataOutput) throws IOException {
        if (dataOutput == null) {
            throw new IllegalArgumentException("The dataOutput cannot be null");
        }
        dataOutput.writeUTF(name);
        dataOutput.writeInt(context.size());
        for (Map.Entry<String, String> entry: context.entrySet()) {
            dataOutput.writeUTF(entry.getKey());
            dataOutput.writeUTF(entry.getValue());
        }
    }

    /**
     * Loads the context's fields from the given DataInput.
     * @param dataInput the stream from which to read
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput == null) {
            throw new IllegalArgumentException("the dataInput cannot be null");
        }
        name = dataInput.readUTF();
        int count = dataInput.readInt();
        for (int i = 0; i < count; i++) {
            context.put(dataInput.readUTF(), dataInput.readUTF());
        }
    }

    /**
     * Serializes the EventContext to a String.
     * @param context the context to serialize
     * @return the serialized EventContext
     */
    public static String serialize(EventContext context) {
        if (context == null) {
            throw new IllegalArgumentException("Event context cannot be null");
        }
        //noinspection SpellCheckingInspection
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream os = new DataOutputStream(baos)) {
            context.write(os);
            os.flush();
            return new String(baos.toByteArray(), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializes an EventContext from a String
     * @param s the String to read
     * @return the EventContext read from the String
     */
    public static EventContext deserialize(String s) {
        if (s == null || s == "") {
            throw new IllegalArgumentException("Dehydrated event context cannot be null or empty");
        }
        try (DataInputStream is = new DataInputStream(new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)))) {
            EventContext ctx = new EventContext("default");
            ctx.readFields(is);
            setCurrent(ctx);
            return ctx;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * An alternative to using the EventContext constructor directly, this method
     * simply creates and returns an EventContext
     * @param name the name for the event context
     * @return the new event context
     */
    public static EventContext enter(String name) {
        return new EventContext(name);
    }

    /**
     * Ends the context. Upon ending, the context's parent context becomes the
     * new current context.
     */
    @Override
    public void close() {
        if (getCurrent() == this) {
            setCurrent(this.parent);
        }
    }

    /**
     * Returns the unique ID for this call chain. The first EventContext to be
     * created on a thread initializes the correlation ID.
     *
     * @return the correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Returns the parent context, which may be null if this is the first
     * context for a thread.
     */
    public EventContext getParent() {
        return parent;
    }

    /**
     * Create an event for the current context.
     *
     * @param severity the severity of the event
     * @param message an enum representing the message key
     * @param substitutions string substitutions that can be substituted
     *                      into the translated message
     * @return an EventBuilder that can be further customized and can generate
     *          an Event that can be logged with EventLogger.
     */
    public static EventBuilder event(Severity severity, Enum message, String... substitutions) {
        return new EventBuilder(getCurrent(), severity, message, substitutions);
    }

    /**
     * Create an event for the current context.
     *
     * @param severity the severity of the event
     * @param message a literal string
     * @param messageCode a numeric identifier for the message
     * @param substitutions string substitutions that can be substituted
     *                      into the message
     * @return an EventBuilder that can be further customized and can generate
     *          an Event that can be logged with EventLogger.
     */
    public static EventBuilder event(Severity severity, int messageCode, String message, String... substitutions) {
        return new EventBuilder(getCurrent(), severity, messageCode, message, substitutions);
    }

    /**
     * Create an event with INFO level severity for the current context.
     *
     * @param message an enum representing the message key
     * @param substitutions string substitutions that can be substituted
     *                      into the translated message
     * @return an EventBuilder that can be further customized and can generate
     *          an Event that can be logged with EventLogger.
     */
    public static EventBuilder event(Enum message, String... substitutions) {
        return event(Severity.INFO, message, substitutions);
    }

}
