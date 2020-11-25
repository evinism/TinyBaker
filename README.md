# tinybaker: Lightweight file-to-file build tool built for production workloads

This is a "working" example of a script that builds an ml model from given train and test dataframes.

```py
# train_step.py
from tinybaker import StepDefinition
import pandas as pd
from some_cool_ml_library import train_model, test_model

class TrainModelStep(StepDefinition):
  input_set = {"train_csv", "test_csv"}
  output_set = {"pickled_model"}

  def script():
    train_data = pd.read_csv(self.input_files["train_csv"])
    test_data = pd.read_csv(self.input_files["test_csv"])
    X = train_data.drop(["label"])
    Y = train_data[["label"]]
    model = train_model(X, Y, depth_or_something=self.config["depth"])
    model.test_model()
    pickle.dump(self.output_files["pickled_model"], model)

```

```py
# script.py
from .train_step import TrainModelStep

[_, train_csv_path, test_csv_path, pickled_model_path] =  parse_args(os)
TrainModelStep.build(
  input={
    "train_csv": train_csv_path,
    "test_csv": test_csv_path,
  },
  output={
    "pickled_model": pickled_model_path
  },
  config={"depth": 5}
)
```

That's it!!
