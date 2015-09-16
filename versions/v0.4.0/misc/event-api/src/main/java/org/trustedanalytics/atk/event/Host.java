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

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Information about the current host MACHINE
 */
class Host {

    private static String MACHINE;
    private static String WORKING_DIRECTORY;
    private static String PROCESS_ID;

    private Host() { }

    static String getMachineName() {
        if (MACHINE == null) {
            MACHINE = lookupMachineName();
        }
        return MACHINE;
    }

    static String getWorkingDirectory() {
        if (WORKING_DIRECTORY == null) {
            WORKING_DIRECTORY = lookupWorkingDirectory();
        }
        return WORKING_DIRECTORY;
    }

    static String getProcessId() {
        if (PROCESS_ID == null) {
            PROCESS_ID = lookupProcessId();
        }
        return PROCESS_ID;
    }

    private static String lookupMachineName() {
        String machineName;
        try {
            machineName = java.net.InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            machineName = "<UNKNOWN>";
        }
        return machineName;
    }

    private static String lookupWorkingDirectory() {
        Path currentRelativePath = Paths.get("");
        return currentRelativePath.toAbsolutePath().toString();
    }

    private static String lookupProcessId() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int loc = name.indexOf('@');
        return name.substring(0, loc);
    }

}
