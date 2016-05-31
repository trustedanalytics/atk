
Consider the following model trained and tested on the sample data set in *frame* 'frame'.

<hide>
>>> import trustedanalytics as ta
>>> ta.connect()
-etc-
>>> frame = ta.Frame(ta.UploadRows([['nytimes','harry',3], ['nytimes','economy',35], ['nytimes','jobs',40], ['nytimes','magic',1],
...                                 ['nytimes','realestate',15], ['nytimes','movies',6],['economist','economy',50],
...                                 ['economist','jobs',35], ['economist','realestate',20],['economist','movies',1],
...                                 ['economist','harry',1],['economist','magic',1],['harrypotter','harry',40],
...                                 ['harrypotter','magic',30],['harrypotter','chamber',20],['harrypotter','secrets',30]],
...                                 [('doc_id', str), ('word_id', str), ('word_count', ta.int64)]))
-etc-

</hide>
Consider the following frame containing three columns.

>>> frame.inspect()
[#]  doc_id     word_id     word_count
======================================
[0]  nytimes    harry                3
[1]  nytimes    economy             35
[2]  nytimes    jobs                40
[3]  nytimes    magic                1
[4]  nytimes    realestate          15
[5]  nytimes    movies               6
[6]  economist  economy             50
[7]  economist  jobs                35
[8]  economist  realestate          20
[9]  economist  movies               1
>>> model = ta.LdaModel()
<progress>
>>> train_output = model.train(frame, 'doc_id', 'word_id', 'word_count', max_iterations = 3, num_topics = 2)
<progress>
<skip>
>>> train_output
{'topics_given_word': Frame  <unnamed>
row_count = 8
schema = [word_id:unicode, topic_probabilities:vector(2)]
status = ACTIVE  (last_read_date = 2015-10-23T11:07:46.556000-07:00), 'topics_given_doc': Frame  <unnamed>
row_count = 3
schema = [doc_id:unicode, topic_probabilities:vector(2)]
status = ACTIVE  (last_read_date = 2015-10-23T11:07:46.369000-07:00), 'report': u'======Graph Statistics======\nNumber of vertices: 11} (doc: 3, word: 8})\nNumber of edges: 16\n\n======LDA Configuration======\nnumTopics: 2\nalpha: 1.100000023841858\nbeta: 1.100000023841858\nmaxIterations: 3\n', 'word_given_topics': Frame  <unnamed>
row_count = 8
schema = [word_id:unicode, topic_probabilities:vector(2)]
status = ACTIVE  (last_read_date = 2015-10-23T11:07:46.465000-07:00)}
</skip>
>>> topics_given_doc = train_output['topics_given_doc']
<progress>
<skip>
>>> topics_given_doc.inspect()
[#]  doc_id       topic_probabilities
===========================================================
[0]  harrypotter  [0.06417509902256538, 0.9358249009774346]
[1]  economist    [0.8065841283073141, 0.19341587169268581]
[2]  nytimes      [0.855316939742769, 0.14468306025723088]
</skip>
>>> topics_given_doc.column_names
[u'doc_id', u'topic_probabilities']
>>> word_given_topics = train_output['word_given_topics']
<progress>
<skip>
>>> word_given_topics.inspect()
[#]  word_id     topic_probabilities
=============================================================
[0]  harry       [0.005015572372943657, 0.2916109787103347]
[1]  realestate  [0.167941871746252, 0.032187084858186256]
[2]  secrets     [0.026543839878055035, 0.17103864163730945]
[3]  movies      [0.03704750433384287, 0.003294403360133419]
[4]  magic       [0.016497495727347045, 0.19676900962555072]
[5]  economy     [0.3805836266747442, 0.10952481503975171]
[6]  chamber     [0.0035944004256137523, 0.13168123398523954]
[7]  jobs        [0.36277568884120137, 0.06389383278349432]
</skip>
>>> word_given_topics.column_names
[u'word_id', u'topic_probabilities']
>>> topics_given_word = train_output['topics_given_word']
<progress>
<skip>
>>> topics_given_word.inspect()
[#]  word_id     topic_probabilities
===========================================================
[0]  harry       [0.018375903962878668, 0.9816240960371213]
[1]  realestate  [0.8663322126823493, 0.13366778731765067]
[2]  secrets     [0.15694172611285945, 0.8430582738871405]
[3]  movies      [0.9444179131148587, 0.055582086885141324]
[4]  magic       [0.09026309091077593, 0.9097369090892241]
[5]  economy     [0.8098866029287505, 0.19011339707124958]
[6]  chamber     [0.0275551649439219, 0.9724448350560781]
[7]  jobs        [0.8748608515169193, 0.12513914848308066]
</skip>
>>> topics_given_word.column_names
[u'word_id', u'topic_probabilities']
>>> prediction = model.predict(['harry', 'secrets', 'magic', 'harry', 'chamber' 'test'])
<progress>
<skip>
>>> prediction
{u'topics_given_doc': [0.3149285399451628, 0.48507146005483726], u'new_words_percentage': 20.0, u'new_words_count': 1}
>>> prediction['topics_given_doc']
[0.3149285399451628, 0.48507146005483726]
>>> prediction['new_words_percentage']
20.0
>>> prediction['new_words_count']
1
</skip>
>>> prediction.has_key('topics_given_doc')
True
>>> prediction.has_key('new_words_percentage')
True
>>> prediction.has_key('new_words_count')
True
>>> model.publish()
<progress>

Take the path to the published model and run it in the Scoring Engine

<skip>
>>> import requests
>>> headers = {'Content-type': 'application/json', 'Accept': 'application/json,text/plain'}
</skip>

Posting a request to get the metadata about the model

<skip>
>>> r =requests.get('http://mymodel.demotrustedanalytics.com/v2/metadata')
>>> r.text
u'{"model_details":{"model_type":"Lda Model","model_class":"org.trustedanalytics.atk.scoring.models.LdaScoreModel","model_reader":"org.trustedanalytics.atk.scoring.models.LdaModelReaderPlugin","custom_values":{}},"input":[{"name":"doc_id","value":"Array[String]"}],"output":[{"name":"doc_id","value":"Array[String]"},{"name":"topics_given_doc","value":"Vector[Double]"},{"name":"new_words_count","value":"Int"},{"name":"new_words_percentage","value":"Double"}]}'
</skip>

The LDA model only supports version 2 of the scoring engine.
Posting a request to version 2 of Scoring Engine supporting Json for requests and responses.

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"doc_id": ['harry', 'secrets', 'magic']}]})
>>> r.text
u'{"data":[{"doc_id":["harry","secrets","magic"],"topics_given_doc":[0.4841745428992676,0.5158254571007324],"new_words_count":0,"new_words_percentage":0.0}]}'
</skip>

Posting a request to version 2 with multiple records to score:

<skip>
>>> r = requests.post("http://mymodel.demotrustedanalytics.com/v2/score", json={"records": [{"doc_id": ['harry', 'secrets', 'magic']}, {"doc_id": ['harry', 'secrets', 'magic']}]})
>>> r.text
u'{"data":[{"doc_id":["harry","secrets","magic"],"topics_given_doc":[0.4841745428992676,0.5158254571007324],"new_words_count":0,"new_words_percentage":0.0},{"doc_id":["harry","secrets","magic"],"topics_given_doc":[0.4841745428992676,0.5158254571007324],"new_words_count":0,"new_words_percentage":0.0}]}'
</skip>