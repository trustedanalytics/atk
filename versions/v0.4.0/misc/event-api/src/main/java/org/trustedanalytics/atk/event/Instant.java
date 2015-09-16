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

import java.util.Date;

/**
 * Encapsulates system data that were current at a particular moment in time
 */
class Instant {

    private final String threadName;
    private final String user;
    private final Date date;
    private final long threadId;

    Instant() {
        date = new Date();
        user = System.getProperty("user.name");
        threadId = Thread.currentThread().getId();
        threadName = Thread.currentThread().getName();
    }

    public String getThreadName() {
        return threadName;
    }

    public String getUser() {
        return user;
    }

    public Date getDate() {
        return date;
    }

    public long getThreadId() {
        return threadId;
    }
}
