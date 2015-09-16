#
# Copyright (c) 2015 Intel Corporation 
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Progress bar printing
"""
import sys
import time
import datetime


class ProgressPrinter(object):

    def __init__(self):
        self.job_count = 0
        self.last_progress = []
        self.job_start_times = []
        self.initializing = True

    def print_progress(self, progress, finished):
        """
        Print progress information on progress bar.

        Parameters
        ----------
        progress : list of dictionary
            The progresses of the jobs initiated by the command

        finished : bool
            Indicate whether the command is finished
        """
        if progress == False:
            return

        total_job_count = len(progress)
        new_added_job_count = total_job_count - self.job_count

        # if it was printing initializing, overwrite initializing in the same line
        # therefore it requires 1 less new line
        number_of_new_lines = new_added_job_count if not self.initializing else new_added_job_count - 1

        if total_job_count > 0:
            self.initializing = False

        for i in range(0, new_added_job_count):
            self.job_start_times.append(time.time())

        self.job_count = total_job_count
        self.print_progress_as_text(progress, number_of_new_lines, self.job_start_times, finished)

    def print_progress_as_text(self, progress, number_of_new_lines, start_times, finished):
        """
        Print progress information on command line progress bar

        Parameters
        ----------
        progress : List of dictionary
            The progresses of the jobs initiated by the command
        number_of_new_lines: int
            number of new lines to print in the command line
        start_times: List of time
            list of observed starting time for the jobs initiated by the command
        finished : boolean
            Indicate whether the command is finished
        """
        if not progress:
            initializing_text = "\rinitializing..."
            sys.stdout.write(initializing_text)
            sys.stdout.flush()
            return len(initializing_text)

        progress_summary = []

        for index in range(0, len(progress)):
            p = progress[index]['progress']
            # Check if the Progress has tasks_info field
            message = ''
            if 'tasks_info' in progress[index].keys():
                retried_tasks = progress[index]['tasks_info']['retries']
                message = "Tasks retries:%s" %(retried_tasks)

            total_bar_length = 25
            factor = 100 / total_bar_length

            num_star = int(p / factor)
            num_dot = total_bar_length - num_star
            number = "%3.2f" % p

            time_string = datetime.timedelta(seconds = int(time.time() - start_times[index]))
            progress_summary.append("\r[%s%s] %6s%% %s Time %s" % ('=' * num_star, '.' * num_dot, number, message, time_string))

        for i in range(0, number_of_new_lines):
            # calculate the index for fetch from the list from the end
            # if number_of_new_lines is 3, will need to take progress_summary[-4], progress_summary[-3], progress_summary[-2]
            # index will be calculated as -4, -3 and -2 respectively
            index = -1 - number_of_new_lines + i
            previous_step_progress = progress_summary[index]
            previous_step_progress = previous_step_progress + "\n"
            sys.stdout.write(previous_step_progress)

        current_step_progress = progress_summary[-1]

        if finished:
            current_step_progress = current_step_progress + "\n"

        sys.stdout.write(current_step_progress)
        sys.stdout.flush()
