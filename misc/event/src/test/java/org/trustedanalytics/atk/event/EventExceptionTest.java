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

import org.junit.Test;

public class EventExceptionTest {
    static enum Msg {
        SOMETHING
    }
    @Test(expected = EventException.class)
    public void EventException_with_event() throws EventException {
        throw new EventException(EventContext.event(Msg.SOMETHING).build());
    }

    @Test(expected = EventException.class)
    public void EventException_with_event_and_cause() throws EventException {
        throw new EventException(EventContext.event(Msg.SOMETHING).build(),
                new Exception());
    }
    @Test(expected = RuntimeEventException.class)
    public void RuntimeEventException_with_event() throws EventException {
        throw new RuntimeEventException(EventContext.event(Msg.SOMETHING).build());
    }

    @Test(expected = RuntimeEventException.class)
    public void RuntimeEventException_with_event_and_cause() throws EventException {
        throw new RuntimeEventException(EventContext.event(Msg.SOMETHING).build(),
                new Exception());
    }
}
