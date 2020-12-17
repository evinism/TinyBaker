import pytest
from tinybaker import Transform, InputTag, OutputTag
from tinybaker.exceptions import FileSetError, BakerError


def test_validate_paths():
    class BasicStep(Transform):
        foo = InputTag("foo")
        bar = InputTag("bar")
        baz = OutputTag("baz")

        def script(self):
            pass

    BasicStep(
        input_paths={"foo": "foo/path", "bar": "bar/path"},
        output_paths={"baz": "baz/path"},
    )

    with pytest.raises(FileSetError):
        BasicStep(
            input_paths={}, output_paths={"baz": "baz/path"}, overwrite=True
        ).run()
    with pytest.raises(FileSetError):
        BasicStep(
            input_paths={"foo": "foo/path", "bar": "bar/path"},
            output_paths={},
            overwrite=True,
        ).run()


def test_opens_local_paths():
    class BasicStep(Transform):
        input_tags = {"foo", "bar"}
        output_tags = {"baz"}

        def script(self):
            with self.input_files["foo"].open() as f:
                assert f.read() == "foo contents"

            with self.input_files["bar"].open() as f:
                assert f.read() == "bar contents"

            with self.output_files["baz"].open() as f:
                f.write("baz contents")

    BasicStep(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bar": "./tests/__data__/bar.txt",
        },
        output_paths={"baz": "./tests/__data__/baz.txt"},
        overwrite=True,
    ).run()


def test_fails_with_missing_paths():
    class BasicStep(Transform):
        input_tags = {"foo", "bar"}
        output_tags = {"baz"}

        def script(self):
            pass

    with pytest.raises(BakerError):
        BasicStep(
            input_paths={
                "foo": "./tests/__data__/foo.txt",
                "faux": "./tests/__data__/bar.txt",
            },
            output_paths={"baz": "./tests/__data__/baz.txt"},
            overwrite=True,
        ).run()


def test_fails_with_circular_inputs():
    class BasicStep(Transform):
        input_tags = {"foo", "bar"}
        output_tags = {"baz"}

        def script(self):
            pass

    with pytest.raises(BakerError):
        BasicStep(
            input_paths={
                "foo": "./tests/__data__/foo.txt",
                "bar": "./tests/__data__/bar.txt",
            },
            output_paths={"baz": "./tests/__data__/foo.txt"},
            overwrite=True,
        ).run()


def test_in_memory_sequence():
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
                f.write(data)

    bar_path = "/tmp/lolol"
    StepOne(
        input_paths={"foo": "./tests/__data__/foo.txt"},
        output_paths={"bar": bar_path},
        overwrite=True,
    ).run()
    StepTwo(
        input_paths={"bar": bar_path}, output_paths={"baz": "/tmp/baz"}, overwrite=True
    ).run()
    with open("/tmp/baz", "r") as f:
        assert f.read() == "foo contents"
