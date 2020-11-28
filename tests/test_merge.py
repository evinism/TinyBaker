from tinybaker import merge, StepDefinition
from tinybaker.exceptions import TagConflictError
import pytest


def test_merge():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    class StepTwo(StepDefinition):
        input_file_set = {"bloop"}
        output_file_set = {"bleep"}

        def script(self):
            with self.input_files["bloop"].open() as f:
                data = f.read()
            with self.output_files["bleep"].open() as f:
                f.write(data + " processed")

    Merged = merge([StepOne, StepTwo])

    Merged(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bloop": "./tests/__data__/bloop.txt",
        },
        output_paths={"bar": "/tmp/bar", "bleep": "/tmp/bleep"},
    ).build(overwrite=True)

    with open("/tmp/bar", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/bleep", "r") as f:
        assert f.read() == "bloop contents processed"


def test_conflicting_outputs():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar", "beep"}

        def script(self):
            pass

    class StepTwo(StepDefinition):
        input_file_set = {"bloop"}
        output_file_set = {"bar", "baz"}

        def script(self):
            pass

    with pytest.raises(TagConflictError):
        merge([StepOne, StepTwo])


def test_conflicting_inputs():
    class StepOne(StepDefinition):
        input_file_set = {"foo", "beppo"}
        output_file_set = {"bar"}

        def script(self):
            pass

    class StepTwo(StepDefinition):
        input_file_set = {"foo", "boppo"}
        output_file_set = {"baz"}

        def script(self):
            pass

    Merged = merge([StepOne, StepTwo])
    Merged.input_file_set = {"foo", "boppo", "beppo"}
    Merged.output_file_set = {"bar", "baz"}
