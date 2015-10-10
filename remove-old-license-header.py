#!/usr/bin/env python
#-*- coding: utf-8 -*-

import sys
import codecs

oldheader = u"""/*
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
*/"""

fname = sys.argv[1]

f = codecs.open(fname, encoding='utf-8', mode='r')
contents = f.read()
f.close()

f2 = codecs.open(fname + '.old', encoding='utf-8', mode='w')
f2.write(contents)
f2.close()

# Remove the old-style license header
contents = contents.replace(oldheader, '')

f3 = codecs.open(fname, encoding='utf-8', mode='w')
f3.write(contents)
f3.close()
