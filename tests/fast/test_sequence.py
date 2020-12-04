import pytest
from tinybaker import sequence, Transform
from tinybaker.context import BakerContext


def test_sequence():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data)

    class StepTwo(Transform):
        input_tags = {"bar"}
        output_tags = {"baz"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

    class StepThree(Transform):
        input_tags = {"baz", "bleep"}
        output_tags = {"boppo"}

        def script(self):
            with self.input_files["baz"].open() as f:
                data = f.read()
            with self.input_files["bleep"].open() as f:
                data2 = f.read()
            with self.output_files["boppo"].open() as f:
                f.write(data + " " + data2)

    Seq = sequence([StepOne, StepTwo, StepThree])

    Seq(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bleep": "./tests/__data__/bleep.txt",
        },
        output_paths={"boppo": "/tmp/boppo"},
        overwrite=True,
    ).run()

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "foo contents processed bleep contents"


def test_in_memory_intermediates():
    in_memory_context = BakerContext(fs_for_intermediates="mem")

    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data)

    class StepTwo(Transform):
        input_tags = {"bar"}
        output_tags = {"baz"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

    class StepThree(Transform):
        input_tags = {"baz", "bleep"}
        output_tags = {"boppo"}

        def script(self):
            with self.input_files["baz"].open() as f:
                data = f.read()
            with self.input_files["bleep"].open() as f:
                data2 = f.read()
            with self.output_files["boppo"].open() as f:
                f.write(data + " " + data2)

    Seq = sequence([StepOne, StepTwo, StepThree])

    Seq(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bleep": "./tests/__data__/bleep.txt",
        },
        output_paths={"boppo": "/tmp/boppo"},
        context=in_memory_context,
        overwrite=True,
    ).run()

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "foo contents processed bleep contents"


def test_complicated_dep_path():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            pass

    class StepTwo(Transform):
        input_tags = {"bar", "bongo"}
        output_tags = {"baz"}

        def script(self):
            pass

    class StepThree(Transform):
        input_tags = {"baz", "bingo"}
        output_tags = {"bop"}

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_tags == {"foo", "bingo", "bongo"}
    assert Seq.output_tags == {"bop"}


def test_exposed_intermediates():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar"}

        def script(self):
            pass

    class StepTwo(Transform):
        input_tags = {"bar"}
        output_tags = {"baz"}

        def script(self):
            pass

    class StepThree(Transform):
        input_tags = {"baz"}
        output_tags = {"bop"}

        def script(self):
            pass

    class StepFour(Transform):
        input_tags = {"bop"}
        output_tags = {"bip"}

        def script(self):
            pass

    Seq = sequence(
        [StepOne, StepTwo, StepThree, StepFour], exposed_intermediates={"bar", "baz"}
    )
    assert Seq.output_tags == {"bip", "bar", "baz"}


def test_gap_in_output_and_input():
    class StepOne(Transform):
        input_tags = {"foo"}
        output_tags = {"bar", "bingo"}

        def script(self):
            pass

    class StepTwo(Transform):
        input_tags = {"bar"}
        output_tags = {"baz"}

        def script(self):
            pass

    class StepThree(Transform):
        input_tags = {"baz", "bingo"}
        output_tags = {"bop"}

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_tags == {"foo"}
    assert Seq.output_tags == {"bop"}
