from tinybaker import Transform, sequence, map_tags, merge, cli
from sklearn import datasets, svm, metrics
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
import pickle

# The digits dataset
digits = datasets.load_digits()


class BuildDf(Transform):
    input_tags = {"raw_images"}
    output_tags = {"df", "labels"}

    def script(self):
        with self.input_files["raw_images"].open() as f:
            data = np.genfromtxt(f, delimiter=",")
        labels = data[:, -1].astype(int)
        images = data[:, 0:-1] / 16.0

        df = pd.DataFrame(images)
        labels_df = pd.DataFrame(data={"label": list(labels)})
        with self.output_files["df"].openbin() as f:
            df.to_pickle(f)
        with self.output_files["labels"].openbin() as f:
            labels_df.to_pickle(f)


class Train(Transform):
    input_tags = {"train_df", "train_labels"}
    output_tags = {"model"}

    def script(self):
        with self.input_files["train_df"].openbin() as f:
            X = pd.read_pickle(f)
        with self.input_files["train_labels"].openbin() as f:
            y = pd.read_pickle(f)

        # Create a classifier: a support vector classifier
        classifier = svm.SVC(gamma=0.001)

        model = classifier.fit(X, y)
        with self.output_files["model"].openbin() as f:
            pickle.dump(model, f)


class Predict(Transform):
    input_tags = {"to_predict_on", "model"}
    output_tags = {"predictions"}

    def script(self):
        with self.input_files["model"].openbin() as f:
            model = pickle.load(f)
        with self.input_files["to_predict_on"].openbin() as f:
            X = pd.read_pickle(f)

        predictions = model.predict(X)
        with self.output_files["predictions"].openbin() as f:
            pickle.dump(predictions, f)


class EvaluateResults(Transform):
    input_tags = {"predictions", "test_labels"}
    output_tags = {"accuracy"}

    def script(self):
        with self.input_files["predictions"].openbin() as f:
            pred = pd.read_pickle(f)
        with self.input_files["test_labels"].openbin() as f:
            real = list(pd.read_pickle(f)["label"])
        total = len(pred)
        correct = 0
        for i in range(total):

            if pred[i] == real[i]:
                correct = correct + 1
        accuracy = round(correct * 1.0 / total, 4)

        with self.output_files["accuracy"].open() as f:
            f.write(str(accuracy))


BuildTrainDf = map_tags(
    BuildDf,
    input_mapping={"raw_images": "raw_train_images"},
    output_mapping={"df": "train_df", "labels": "train_labels"},
)
BuildTestDf = map_tags(
    BuildDf,
    input_mapping={"raw_images": "raw_test_images"},
    output_mapping={"df": "to_predict_on", "labels": "test_labels"},
)

Pipeline = sequence(
    [
        merge([sequence([BuildTrainDf, Train]), BuildTestDf]),
        Predict,
        EvaluateResults,
    ],
    exposed_intermediates={"model"},
    name="MNISTPipeline",
)


def test_real_world():
    Pipeline(
        input_paths={
            "raw_train_images": "./tests/__data__/mnist/optdigits.tra",
            "raw_test_images": "./tests/__data__/mnist/optdigits.tes",
        },
        output_paths={"accuracy": "/tmp/accuracy", "model": "/tmp/model"},
        overwrite=True,
    ).run()

    with open("/tmp/accuracy", "r") as f:
        assert f.read() == "0.9032"


if __name__ == "__main__":
    cli(Pipeline)