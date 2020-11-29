# tinybaker: Lightweight tool for defining composable file-to-file transformations

### Warning: tinybaker is still in alpha, and is not yet suitable for production use

Installation with pip, e.g. `pip install tinybaker`

tinybaker allows programmers to define file-to-file transformations in a simple, concise format, and compose them together with clarity. 


## Anatomy of a single step

Let's say we wanted to define a transformation from one set of files to another. Tinybaker allows a developer to specify a set of input file "tags" that are specified independently from the transformation declaration.

```py
from tinybaker import Transform

class SampleTransform(Transform):
  # 1 tag per input file for this transformation
  input_tags = {"first_input", "second_input"}
  output_tags = {"some_output"}

  # self.script describes what actually executes when the transform task runs
  script(self):
    # Transforms provide self.input_files and self.output_files, dictionaries with
    # fully-qualified references to files that can be directly opened:
    with self.input_files["first_input"].open() as f:
      do_something_with(f)
    with self.input_files["second_input"].open() as f:
      do_something_else_with(f)

    # and output or something
    with self.input_files["some_output"].open() as f:
      write_something_to(f)

```

This would then be executed via:

```py

SampleTransform(
  input_paths={"first_input": "path/to/input1", "second_input"= "path/to/input2"}
  output_paths={"some_output": "path/to/write/output"}
).build()

```

### Real-world example

For a real-world example, consider training an ML model. This is a transformation from the two files `some/path/train.csv` and `some/path/test.csv` to a pickled ML model `another/path/some_model.pkl` and statistics. With `tinybaker`, you can specify this individual configurable step as follows:

```py
# train_step.py
from tinybaker import Transform
import pandas as pd
from some_cool_ml_library import train_model, test_model

class TrainModelStep(Transform):
  input_tags = {"train_csv", "test_csv"}
  output_tags = {"pickled_model", "results"}

  def script():
    # Read from files
    with self.input_files["train_csv"].open() as f:
      train_data = pd.read_csv(f)
    with self.input_files["test_csv"].open() as f:
      test_data = pd.read_csv(f)

    # Run computations
    X = train_data.drop(["label"])
    Y = train_data[["label"]]
    [model, train_results] = train_model(X, Y)
    test_results = test_model(model, test_data)

    # Write to output files
    with self.output_files["results"] as f:
      results = train_results.formatted_sumarry() + test_results.formatted_summary()
      f.write(results)
    with self.output_files["pickled_model"] as f:
      pickle.dump(f, model)

```

The script that consumes this may look like:

```py
# script.py
from .train_step import TrainModelStep

[_, train_csv_path, test_csv_path, pickled_model_path, results_path] =  parse_args(os)
TrainModelStep(
  input_paths={
    "train_csv": train_csv_path,
    "test_csv": test_csv_path,
  },
  output_paths={
    "pickled_model": pickled_model_path,
    "results": results_path
  }
).build()
```

This will perform standard error handling, such as raising early if certain files are missing.

## Combining several build steps

Let's say you've got a sequence of steps. We can compose several build steps together using the methods `merge` and `sequence`.

```
from tinybaker import Transform, sequence

class CleanLogs(Transform):
  input_files={"raw_logfile"}
  output_files={"cleaned_logfile"}
  ...

class BuildDataframe(Transform):
  input_files={"cleaned_logfile"}
  output_files={"dataframe"}
  ...

class BuildLabels(Transform):
  input_files={"cleaned_logfile"}
  output_files={"labels"}

class TrainModelFromDataframe(Transform):
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

Hooking up inputs and outputs is determined via tag name, e.g. if step 1 outputs tag "foo", and step 2 takes tag "foo" as inputs, they will be automatically hooked together.

### Propagation of inputs and outptus
Let's say task 3 of 4 in a sequence of tasks requires tag "foo", but no previous step generates tag "foo", then this dependency will be propagated to the top level; the sequence as a whole will have a dependency on tag "foo".

Additionally, if task 3 of 4 generates a tag "bar", but no further step requires "bar", then the sequence exposes "bar" as an output.

### expose_intermediates
If you need to expose intermediate files within a sequence, you can use the keywork arg `expose_intermediates` to additionally output the listed intermediate tags, e.g.

`sequence([A, B, C], expose_intermediates={"some_intermediate", "some_other_intermediate"})`

## Renaming

Right now, since association of files from one step to the next is based on tags, we may end up in a situation where we want to rename tags. If we want to change the tag names, we can use `map_tags` to change them.

```
MappedStep = map_tags(
  SomeStep,
  input_mapping={"old_input_name": "new_input_name"},
  output_mapping={"old_output_name": "new_output_name"})
```

