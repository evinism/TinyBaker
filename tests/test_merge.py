from tinybaker import merge, Transform
from tinybaker.exceptions import TagConflictError
import pytest
from tests.runtime import runtime


def test_merge():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    class StepTwo(Transform):
        input_tags = {"bloop"}
        output_tags = {"bleep"}

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
    ).build(runtime)

    with open("/tmp/bar", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/bleep", "r") as f:
        assert f.read() == "bloop contents processed"


def test_conflicting_outputs():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar", "beep"}

        def script(self):
            pass

    class StepTwo(Transform):
        input_tags = {"bloop"}
        output_tags = {"bar", "baz"}

        def script(self):
            pass

    with pytest.raises(TagConflictError):
        merge([StepOne, StepTwo])


def test_conflicting_inputs():
    class StepOne(Transform):
        input_tags = {"foo", "beppo"}
        output_tags = {"bar"}

        def script(self):
            pass

    class StepTwo(Transform):
        input_tags = {"foo", "boppo"}
        output_tags = {"baz"}

        def script(self):
            pass

    Merged = merge([StepOne, StepTwo])
    Merged.input_tags = {"foo", "boppo", "beppo"}
    Merged.output_tags = {"bar", "baz"}
