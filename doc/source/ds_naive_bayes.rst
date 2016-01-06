
    def test_model_class_doc(self):
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

        # The general workflow will be build a frame, build a model,
        # train the model on the frame, predict using the model,
        # evaluate the results using classification metrics

        # First Step, construct a frame
        # Construct a frame to be uploaded, this is done using plain python
        # lists uploaded to the server

        # Each row represents a sample from a probability distribution,
        # with a vector associated with a category. For the purposes of this
        # example there are two categories (e.g. cat and dog) and three
        # features to indicate whether the sample is a cat or a dog (say
        # mass, height, fur type)

        # The frame has the schema of
        # Class, feature 1, feature 2, feature 3
        # where class is the category that the sample belongs to
        rows_frame = ia.UploadRows([[0,1,0,0],
                                    [0,2,0,0],
                                    [1,0,1,0],
                                    [1,0,2,0]],
                                   [("class", ia.float32),
                                    ("f1", ia.int32),
                                    ("f2", ia.int32),
                                    ("f3", ia.int32)])
        # Actually build the frame described in in the UploadRows object
        frame = ia.Frame(rows_frame)

        print frame.inspect()

        # Build a model
        # Naive Bayes is a basic classifier
        nb_model = ia.NaiveBayesModel()

        # Train the model on the frame, this is supervised training technique
        # so the category is used in the training process. Note the feature
        # vector is represented as a list of column names.
        nb_model.train(frame, "class", ["f1", "f2", "f3"])

        # Predict assigns a category to a sample in the feature space
        # For the purposes of illustrating the workflow, I am predicting on the
        # same frame used to train, normally you would predict on a different
        # frame representing data that didn't have a category assigned to it

        # again note the feature vector is a python list of column names
        result = nb_model.predict(frame, ["f1", "f2", "f3"])

        # The result is a frame with a new "predicted_class" column
        print result.inspect()

        # Run classification metrics on the resultant frame to understand
        # model performance
        cm = result.classification_metrics("class", "predicted_class")

        print cm

        # Assert the results are correct
        self.assertEqual(cm.confusion_matrix.values[0][0],2)
        self.assertEqual(cm.confusion_matrix.values[1][1],2)
        self.assertEqual(cm.confusion_matrix.values[0][1],0)
        self.assertEqual(cm.confusion_matrix.values[1][0],0)

