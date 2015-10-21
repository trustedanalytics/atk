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

import trustedanalytics as ta

ta.connect()


print("define csv file")
csv = ta.CsvFile("/lda.csv", schema= [('doc_id', str),
                                        ('word_id', str),
                                        ('word_count', ta.int64)], skip_header_lines=1)
print("create frame")
frame = ta.Frame(csv)

print("inspect frame")
frame.inspect(20)
print("frame row count " + str(frame.row_count))

model = ta.GiraphLdaModel()
results = model.train(frame,
            'doc_id', 'word_id', 'word_count',
            max_iterations = 3,
            num_topics = 2)

topics_given_doc = results['topics_given_doc']
word_given_topics = results['word_given_topics']
topics_given_word = results['topics_given_word']
report = results['report']

topics_given_doc.inspect()
word_given_topics.inspect()
topics_given_word.inspect()
print report

print("compute topic probabilities for document")
prediction = model.predict(['harry', 'economy', 'magic', 'harry' 'test'])
print(prediction)

print("compute lda score")
topics_given_doc.rename_columns({'topic_probabilities' : 'lda_topic_given_doc'})
word_given_topics.rename_columns({'topic_probabilities' : 'lda_word_given_topic'})

frame= frame.join(topics_given_doc, left_on="doc_id", right_on="doc_id", how="left")
frame= frame.join(word_given_topics, left_on="word_id", right_on="word_id", how="left")

frame.dot_product(['lda_topic_given_doc'], ['lda_word_given_topic'], 'lda_score')
frame.inspect()

print("compute histogram of scores")
word_hist = frame.histogram('word_count')
lda_hist = frame.histogram('lda_score')
group_frame = frame.group_by('word_id', {'word_count': ta.agg.histogram(word_hist.cutoffs), 'lda_score':  ta.agg.histogram(lda_hist.cutoffs)})
group_frame.inspect()
