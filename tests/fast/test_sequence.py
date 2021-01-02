import pytest
from tinybaker import sequence, Transform, InputTag, OutputTag
from tinybaker.context import BakerDriverContext


def test_sequence():
    class StepOne(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            with self.foo.open() as f:
                data = f.read()
            with self.bar.open() as f:
                f.write(data)

    class StepTwo(Transform):
        bar = InputTag("bar")
        baz = OutputTag("baz")

        def script(self):
            with self.bar.open() as f:
                data = f.read()
            with self.baz.open() as f:
                f.write(data + " processed")

    class StepThree(Transform):
        baz = InputTag("baz")
        bleep = InputTag("bleep")
        boppo = OutputTag("boppo")

        def script(self):
            with self.baz.open() as f:
                data = f.read()
            with self.bleep.open() as f:
                data2 = f.read()
            with self.boppo.open() as f:
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
    in_memory_context = BakerDriverContext(fs_for_intermediates="mem")

    class StepOne(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            with self.foo.open() as f:
                data = f.read()
            with self.bar.open() as f:
                f.write(data)

    class StepTwo(Transform):
        bar = InputTag("bar")
        baz = OutputTag("baz")

        def script(self):
            with self.bar.open() as f:
                data = f.read()
            with self.baz.open() as f:
                f.write(data + " processed")

    class StepThree(Transform):
        baz = InputTag("baz")
        bleep = InputTag("bleep")
        boppo = OutputTag("boppo")

        def script(self):
            with self.baz.open() as f:
                data = f.read()
            with self.bleep.open() as f:
                data2 = f.read()
            with self.boppo.open() as f:
                f.write(data + " " + data2)

    Seq = sequence([StepOne, StepTwo, StepThree])

    in_memory_context.run(
        Seq(
            input_paths={
                "foo": "./tests/__data__/foo.txt",
                "bleep": "./tests/__data__/bleep.txt",
            },
            output_paths={"boppo": "/tmp/boppo"},
            overwrite=True,
        )
    )

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "foo contents processed bleep contents"


def test_complicated_dep_path():
    class StepOne(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            pass

    class StepTwo(Transform):
        bar = InputTag("bar")
        bongo = InputTag("bongo")
        baz = OutputTag("baz")

        def script(self):
            pass

    class StepThree(Transform):
        baz = InputTag("baz")
        bleep = InputTag("bingo")
        boppo = OutputTag("bop")

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_tags == {"foo", "bingo", "bongo"}
    assert Seq.output_tags == {"bop"}


def test_exposed_intermediates():
    class StepOne(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            pass

    class StepTwo(Transform):
        bar = InputTag("bar")
        baz = OutputTag("baz")

        def script(self):
            pass

    class StepThree(Transform):
        baz = InputTag("baz")
        bop = OutputTag("bop")

        def script(self):
            pass

    class StepFour(Transform):
        bop = InputTag("bop")
        bip = OutputTag("bip")

        def script(self):
            pass

    Seq = sequence(
        [StepOne, StepTwo, StepThree, StepFour], exposed_intermediates={"bar", "baz"}
    )
    assert Seq.output_tags == {"bip", "bar", "baz"}


def test_gap_in_output_and_input():
    class StepOne(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")
        bingo = OutputTag("bingo")

        def script(self):
            pass

    class StepTwo(Transform):
        bar = InputTag("bar")
        baz = OutputTag("baz")

        def script(self):
            pass

    class StepThree(Transform):
        baz = InputTag("baz")
        bingo = InputTag("bingo")
        bop = OutputTag("bop")

        def script(self):
            pass

    Seq = sequence([StepOne, StepTwo, StepThree])
    assert Seq.input_tags == {"foo"}
    assert Seq.output_tags == {"bop"}
