import pytest
from tinybaker import StepDefinition
from tinybaker.exceptions import FileSetError, BakerError


def test_validate_paths():
    class BasicStep(StepDefinition):
        input_file_set = {"foo", "bar"}
        output_file_set = {"baz"}

        def script(self):
            pass

    BasicStep(
        input_paths={"foo": "foo/path", "bar": "bar/path"},
        output_paths={"baz": "baz/path"},
    )

    with pytest.raises(FileSetError):
        BasicStep(input_paths={}, output_paths={"baz": "baz/path"})
    with pytest.raises(FileSetError):
        BasicStep(input_paths={"foo": "foo/path", "bar": "bar/path"}, output_paths={})


def test_opens_local_paths():
    class BasicStep(StepDefinition):
        input_file_set = {"foo", "bar"}
        output_file_set = {"baz"}

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
    ).build(overwrite=True)


def test_fails_with_missing_paths():
    class BasicStep(StepDefinition):
        input_file_set = {"foo", "bar"}
        output_file_set = {"baz"}

        def script(self):
            pass

    with pytest.raises(BakerError):
        BasicStep(
            input_paths={
                "foo": "./tests/__data__/foo.txt",
                "faux": "./tests/__data__/bar.txt",
            },
            output_paths={"baz": "./tests/__data__/baz.txt"},
        ).build()


def test_fails_with_circular_inputs():
    class BasicStep(StepDefinition):
        input_file_set = {"foo", "bar"}
        output_file_set = {"baz"}

        def script(self):
            pass

    with pytest.raises(BakerError):
        BasicStep(
            input_paths={
                "foo": "./tests/__data__/foo.txt",
                "bar": "./tests/__data__/bar.txt",
            },
            output_paths={"baz": "./tests/__data__/foo.txt"},
        ).build()


def test_in_memory_sequence():
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
                f.write(data)

    bar_path = "/tmp/lolol"
    StepOne(
        input_paths={"foo": "./tests/__data__/foo.txt"}, output_paths={"bar": bar_path}
    ).build(overwrite=True)
    StepTwo(input_paths={"bar": bar_path}, output_paths={"baz": "/tmp/baz"}).build(
        overwrite=True
    )
    with open("/tmp/baz", "r") as f:
        assert f.read() == "foo contents"
