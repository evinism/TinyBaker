import pytest
from xforge import StepDefinition
from xforge.exceptions import FileSetError


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
    ).build()
