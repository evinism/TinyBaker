import pytest
from tinybaker import sequence, StepDefinition


def test_sequence():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data)

    class StepTwo(StepDefinition):
        input_file_set = {"bar"}
        output_file_set = {"baz"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

    class StepThree(StepDefinition):
        input_file_set = {"baz", "bleep"}
        output_file_set = {"boppo"}

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
    ).build(overwrite=True)

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "foo contents processed bleep contents"


def test_complicated_dep_path():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar"}

        def script(self):
            pass

    class StepTwo(StepDefinition):
        input_file_set = {"bar", "bongo"}
        output_file_set = {"baz"}

        def script(self):
            pass

    class StepThree(StepDefinition):
        input_file_set = {"baz", "bingo"}
        output_file_set = {"bop"}

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_file_set == {"foo", "bingo", "bongo"}
    assert Seq.output_file_set == {"bop"}


def test_exposed_intermediates():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar"}

        def script(self):
            pass

    class StepTwo(StepDefinition):
        input_file_set = {"bar"}
        output_file_set = {"baz"}

        def script(self):
            pass

    class StepThree(StepDefinition):
        input_file_set = {"baz"}
        output_file_set = {"bop"}

        def script(self):
            pass

    class StepFour(StepDefinition):
        input_file_set = {"bop"}
        output_file_set = {"bip"}

        def script(self):
            pass

    Seq = sequence(
        [StepOne, StepTwo, StepThree, StepFour], exposed_intermediates={"bar", "baz"}
    )
    assert Seq.output_file_set == {"bip", "bar", "baz"}


def test_gap_in_output_and_input():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar", "bingo"}

        def script(self):
            pass

    class StepTwo(StepDefinition):
        input_file_set = {"bar"}
        output_file_set = {"baz"}

        def script(self):
            pass

    class StepThree(StepDefinition):
        input_file_set = {"baz", "bingo"}
        output_file_set = {"bop"}

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_file_set == {"foo"}
    assert Seq.output_file_set == {"bop"}
