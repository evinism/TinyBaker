# tinybaker: Lightweight file-to-file build tool built for production workloads

Installation with pip, e.g. `pip install tinybaker`

## Brief Example

Let's say we wanted to define a transformation from one set of files to another. Tinybaker allows a developer to specify a set of input "tags" that can be configured later.

For example, consider a transformation from the two files `some/path/train.csv` and `some/path/test.csv` to a pickled ML model `another/path/some_model.pkl`. With `tinybaker`, you can specify this individual configurable step as follows:

```py
# train_step.py
from tinybaker import StepDefinition
import pandas as pd
from some_cool_ml_library import train_model, test_model

class TrainModelStep(StepDefinition):
  input_file_set = {"train_csv", "test_csv"}
  output_file_set = {"pickled_model"}

  def script():
    with self.input_files["train_csv"].open() as f:
      train_data = pd.read_csv(f)
    with self.input_files["test_csv"].open() as f:
      test_data = pd.read_csv(f)
    X = train_data.drop(["label"])
    Y = train_data[["label"]]
    model = train_model(X, Y, depth_or_something=self.config["depth"])
    model.test_model()
    with self.output_files["pickled_model"] as f:
      pickle.dump(f, model)

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

This will perform standard error handling, such as raising early if certain files are missing.

## Combining several build steps

Let's say you've got a sequence of steps. We can compose several build steps together using the methods `merge` and `sequence`.

```
from tinybaker import StepDefinition, sequence

class CleanLogs(StepDefinition):
  input_files={"raw_logfile"}
  output_files={"cleaned_logfile"}
  ...

class BuildDataframe(StepDefinition):
  input_files={"cleaned_logfile"}
  output_files={"dataframe"}
  ...

class BuildLabels(StepDefinition):
  input_files={"cleaned_logfile"}
  output_files={"labels"}

class TrainModelFromDataframe(StepDefinition):
  input_files={"dataframe", "labels"}
  output_files={"trained_model"}


TrainFromRawLogs = sequence(
  CleanLogs,
  merge(BuildDataframe, BuildLabels),
  TrainModelFromDataframe
)

task = TrainFromRawLogs(
  input_paths={"raw_logfile": "/path/to/raw.log"},
  output_paths={"trained_model": "/path/to/model.pkl"}
)

task.build()

```

## Mapping

Right now, association of files from one step to the next is based on tags. If we want to change the tag names, we can use `map_tags` to change them.

```
MappedStep = map_tags(
  SomeStep,
  input_mapping={"old_input_name": "new_input_name"},
  output_mapping={"old_output_name": "new_output_name"})
```

## That's it!!
