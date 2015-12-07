# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import datetime

def parse_for_doc(text):
    return str(DocExamplesPreprocessor(text, mode='doc'))


def parse_for_doctest(text):
    return str(DocExamplesPreprocessor(text, mode='doctest'))


class Doc(object):
    """Represents descriptive text for an object, but not its individual pieces"""

    def __init__(self, one_line='<Missing Doc>', extended='', examples=None):
        self.one_line = one_line.strip()
        self.extended = extended
        self.examples = parse_for_doc(examples)

    def __str__(self):
        r = self.one_line
        if self.extended:
            r += ("\n\n" + self.extended)
        if self.examples:
            r += ("\n\n" + self.examples)
        return r

    def __repr__(self):
        import json
        return json.dumps(self.__dict__)

    @staticmethod
    def _pop_blank_lines(lines):
        while lines and not lines[0].strip():
            lines.pop(0)

    @staticmethod
    def get_from_str(doc_str):
        if doc_str:
            lines = doc_str.split('\n')

            Doc._pop_blank_lines(lines)
            summary = lines.pop(0).strip() if lines else ''
            Doc._pop_blank_lines(lines)
            if lines:
                margin = len(lines[0]) - len(lines[0].lstrip())
                extended = '\n'.join([line[margin:] for line in lines])
            else:
                extended = ''

            if summary:
                return Doc(summary, extended)

        return Doc("<Missing Doc>", doc_str)


class DocExamplesException(Exception):
    """Exception specific to processing documentation examples"""
    pass


class DocExamplesPreprocessor(object):
    """
    Processes text (intended for Documentation Examples) and applies ATK doc markup, mostly to enable doctest testing
    """

    doctest_ellipsis = '-etc-'  # override for the doctest ELLIPSIS_MARKER

    # multi-line tags
    hide_start_tag = '<hide>'
    hide_stop_tag = '</hide>'
    skip_start_tag = '<skip>'
    skip_stop_tag = '</skip>'

    # replacement tags
    doc_replacements = [('<progress>', '[===Job Progress===]'),
                        ('<connect>', 'Connected ...'),
                        ('<datetime.datetime>', repr(datetime.datetime.now())),
                        ('<blankline>', '<BLANKLINE>')]   # sphinx will ignore this for us

    doctest_replacements = [('<progress>', doctest_ellipsis),
                            ('<connect>', doctest_ellipsis),
                            ('<datetime.datetime>', doctest_ellipsis),
                            ('<blankline>', '<BLANKLINE>')]

    # Two simple fsms, each with 2 states:  Keep, Drop
    keep = 0
    drop = 1

    def __init__(self, text, mode='doc'):
        """
        :param text: str of text to process
        :param mode:  preprocess mode, like 'doc' or 'doctest'
        :return: object whose __str__ is the processed example text
        """
        if mode == 'doc':
            # process for human-consumable documentation
            self.replacements = self.doc_replacements
            self.is_state_keep = self._is_hide_state_keep
        elif mode == 'doctest':
            # process for doctest execution
            self.replacements = self.doctest_replacements
            self.is_state_keep = self._is_skip_state_keep
        else:
            raise DocExamplesException('Invalid mode "%s" given to %s.  Must be in %s' %
                                       (mode, self.__class__, ", ".join(['doc', 'doctest'])))
        self.skip_state = self.keep
        self.hide_state = self.keep
        self.processed = ''

        if text:
            lines = text.splitlines(True)
            self.processed = ''.join(self._process_line(line) for line in lines)
            if self.hide_state != self.keep:
                raise DocExamplesException("unclosed tag %s found" % self.hide_start_tag)
            if self.skip_state != self.keep:
                raise DocExamplesException("unclosed tag %s found" % self.skip_start_tag)

    def _is_skip_state_keep(self):
        return self.skip_state == self.keep

    def _is_hide_state_keep(self):
        return self.hide_state == self.keep

    def _process_line(self, line):
        """processes line and advances fsms as necessary, returns processed line text"""
        stripped = line.lstrip()
        if stripped and stripped[0] == '<':
            if self._process_if_tag_pair_tag(stripped):
                return ''  # return empty string, as tag-pari markup should disappear

            # check for keyword replacement
            for keyword, replacement in self.replacements:
                if stripped.startswith(keyword):
                    line = line.replace(keyword, replacement, 1)
                    break

        return line if self.is_state_keep() else ''

    def _process_if_tag_pair_tag(self, stripped):
        """determines if the stripped line is a tag pair start or stop, advances fsms accordingly"""
        if stripped.startswith(self.skip_start_tag):
            if self.skip_state == self.drop:
                raise DocExamplesException("nested tag %s found" % self.skip_start_tag)
            self.skip_state = self.drop
            return True
        elif stripped.startswith(self.skip_stop_tag):
            if self.skip_state == self.keep:
                raise DocExamplesException("unexpected tag %s found" % self.skip_stop_tag)
            self.skip_state = self.keep
            return True
        elif stripped.startswith(self.hide_start_tag):
            if self.hide_state == self.drop:
                raise DocExamplesException("nested tag %s found" % self.hide_start_tag)
            self.hide_state = self.drop
            return True
        elif stripped.startswith(self.hide_stop_tag):
            if self.hide_state == self.keep:
                raise DocExamplesException("unexpected tag %s found" % self.hide_stop_tag)
            self.hide_state = self.keep
            return True
        return False

    def __str__(self):
        return self.processed
