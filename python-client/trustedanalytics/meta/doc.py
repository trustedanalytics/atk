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

    valid_modes = ['doc',        # process for human-consumable documentation
                   'doctest',    # process for doctest execution
                  ]

    doctest_ellipsis = '-etc-'  # override for the doctest ELLIPSIS_MARKER

    # multi-line tags
    hide_start_tag = '<hide>'
    hide_stop_tag = '</hide>'
    skip_start_tag = '<skip>'
    skip_stop_tag = '</skip>'

    # replacement tags
    doc_replacements = [('<progress>', '[===Job Progress===]'),
                        ('<connect>', 'Connected ...'),
                        ('<blankline>', '<BLANKLINE>')]   # sphinx will ignore this for us

    doctest_replacements = [('<progress>', doctest_ellipsis),
                            ('<connect>', doctest_ellipsis),
                            ('<blankline>', '<BLANKLINE>')]

    # this is a simple 2-state fsm:  Keep, Drop
    keep = 0
    drop = 1

    def __init__(self, text, mode='doc'):
        """
        :param text: str of text to process
        :param mode:  preprocess mode, like 'doc' or 'doctest'
        :return: object whose __str__ is the processed example text
        """
        if mode not in self.valid_modes:
            raise DocExamplesException('Invalid mode "%s" given to %s.  Must be in %s' %
                                       (mode, self.__class__, ", ".join(self.valid_modes)))

        self.state = self.keep
        self.processed = ''

        if text:

            process = self.process_for_doc if mode == 'doc' else self.process_for_doctest
            lines = text.splitlines(True)
            self.processed = ''.join(process(line) for line in lines)
            if self.state != self.keep:
                raise DocExamplesException("unclosed tag %s found" % self.hide_start_tag)

    def process_for_doc(self, line):
        """process line specifically for documentation"""
        stripped = line.lstrip()
        if stripped and stripped[0] == '<':
            if stripped.startswith(self.hide_start_tag):
               if self.state == self.drop:
                   raise DocExamplesException("nested tag %s found" % self.hide_start_tag)
               self.state = self.drop
               return ''
            elif stripped.startswith(self.hide_stop_tag):
                if self.state == self.keep:
                    raise DocExamplesException("unexpected tag %s found" % self.hide_stop_tag)
                self.state = self.keep
                return ''
            elif stripped.startswith(self.skip_start_tag) or line.startswith(self.skip_stop_tag):
                return ''
            if self.state == self.keep:
                for keyword, replacement in self.doc_replacements:
                    if stripped.startswith(keyword):
                        return line.replace(keyword, replacement, 1)
        return line if self.state == self.keep else ''

    def process_for_doctest(self, line):
        """process line specifically for doctest execution"""
        stripped = line.lstrip()
        if stripped and stripped[0] == '<':
            if stripped.startswith(self.skip_start_tag):
               if self.state == self.drop:
                   raise DocExamplesException("nested tag %s found" % self.skip_start_tag)
               self.state = self.drop
               return ''
            elif stripped.startswith(self.skip_stop_tag):
                if self.state == self.keep:
                    raise DocExamplesException("unexpected tag %s found" % self.skip_stop_tag)
                self.state = self.keep
                return ''
            if self.state == self.keep:
                for keyword, replacement in self.doctest_replacements:
                    if stripped.startswith(keyword):
                        return line.replace(keyword, replacement, 1)
        return line if self.state == self.keep else ''

    def __str__(self):
        return self.processed
