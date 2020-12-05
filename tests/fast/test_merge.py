from tinybaker import sequence, merge, Transform, BakerContext, map_tags
from tinybaker.exceptions import TagConflictError
import pytest


def test_merge():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    StepTwo = map_tags(StepOne, {"foo": "bloop"}, {"bar": "bleep"})

    Merged = merge([StepOne, StepTwo])

    Merged(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bloop": "./tests/__data__/bloop.txt",
        },
        output_paths={"bar": "/tmp/bar", "bleep": "/tmp/bleep"},
        overwrite=True,
    ).run()

    with open("/tmp/bar", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/bleep", "r") as f:
        assert f.read() == "bloop contents processed"


def test_merge_and_sequence():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    class Id(Transform):
        input_tags = {"boppo", "baz"}
        output_tags = {"foo", "bloop"}

        def script(self):
            with self.input_files["boppo"].open() as x:
                with self.output_files["foo"].open() as y:
                    y.write(x.read())
            with self.input_files["baz"].open() as x:
                with self.output_files["bloop"].open() as y:
                    y.write(x.read())

    StepTwo = map_tags(StepOne, {"foo": "bloop"}, {"bar": "bleep"})

    Merged = sequence([Id, merge([StepOne, StepTwo])])

    Merged(
        input_paths={
            "boppo": "./tests/__data__/foo.txt",
            "baz": "./tests/__data__/bloop.txt",
        },
        output_paths={"bar": "/tmp/bar", "bleep": "/tmp/bleep"},
        overwrite=True,
    ).run()

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


def test_not_parallel_merge():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    StepTwo = map_tags(StepOne, {"foo": "bloop"}, {"bar": "bleep"})

    Merged = merge([StepOne, StepTwo])

    Merged(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bloop": "./tests/__data__/bloop.txt",
        },
        output_paths={"bar": "/tmp/bar", "bleep": "/tmp/bleep"},
        overwrite=True,
        context=BakerContext(parallel_mode=None),
    ).run()

    with open("/tmp/bar", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/bleep", "r") as f:
        assert f.read() == "bloop contents processed"
