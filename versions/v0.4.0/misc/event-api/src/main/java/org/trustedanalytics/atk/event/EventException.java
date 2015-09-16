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
 * An exception that packages an Event as its payload
 */
public class EventException extends Exception {

    private final Event event;

    /**
     * Constructor for an event
     */
    public EventException(Event event) {
        this.event = event;
    }

    /**
     * Constructor for the case where the error was caused by an earlier
     * error.
     */
    public EventException(Event event, Throwable cause) {
        super(cause);
        this.event = event;
    }

    /**
     * The event that caused this exception
     * @return the associated event
     */
    public Event getEvent() {
        return this.event;
    }
}
