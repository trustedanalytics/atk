        """Documentation test for classifiers"""
        # Establish a connection to the ATK Rest Server
        # This handle will be used for the remaineder of the script
        # No cleanup is required

        # First you have to get your server URL and credentials file
        # from your TAP administrator
        atk_server_uri = os.getenv("ATK_SERVER_URI", ia.server.uri)
        credentials_file = os.getenv("ATK_CREDENTIALS", "")

        # set the server, and use the credentials to connect to the ATK
        ia.server.uri = atk_server_uri
        ia.connect(credentials_file)

        # LDA performs what is known as topic modeling, which is a form of
        # clustering. The conceptual idea is you have some number of 
        # documents, each which contain some number of words. Based on the
        # words you want to associate each document with a particular topic.
        # This algorithm is unsupervised, meaning there is no known values
        # that are being trained against, rather it simply associates similar
        # papers, where similarity is defined as having similar words.
        # The number of times a word occurs in a paper is also taken into
        # account

        # The general workflow will be build a frame, build a model,
        # train the model on the frame,
        # Predict on the model. Note there's no metrics, evaluating
        # unsupervised machine learning results can be difficult

        # First Step, construct a frame
        # Construct a frame to be uploaded, this is done using plain python
        # lists uploaded to the server


        # I am representing 3 papers on 2 topics, with 4 words each, which
        # each appear 2 times

        rows_frame = ia.UploadRows([["paper1", "word11", 2],
                                    ["paper1", "word12", 2],
                                    ["paper1", "word13", 2],
                                    ["paper1", "word14", 2],

                                    ["paper2", "word11", 2],
                                    ["paper2", "word12", 2],
                                    ["paper2", "word13", 2],
                                    ["paper2", "word14", 2],

                                    ["paper3", "word11", 2],
                                    ["paper3", "word22", 2],
                                    ["paper3", "word23", 2],
                                    ["paper3", "word24", 2]],
                                   [("paper", str),
                                    ("word", str),
                                    ("count", ia.int32)])
        # Actually build the frame described in in the UploadRows object
        frame = ia.Frame(rows_frame)

        print frame.inspect()

        # Build a model
        # This lda model will be trained against the above frame
        lda_model = ia.LdaModel()

        # Give the model the papers to train topics on, and words associated
        # with those papers, and the count of words in a paper. The final
        # argument is the number of topics to search for.
        lda_model.train(frame, "paper", "word", "count", num_topics=2)

        # predict the words of 2 new papers, and show that they are in different
        # topics. A new paper is just a list of words and counts of those
        # words
        cluster1 = lda_model.predict(["word11", "word13"])
        cluster2 = lda_model.predict(["word21", "word23"])
        print cluster1
        print cluster2

        self.assertNotEqual(cluster1, cluster2)


