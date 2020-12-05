import pytest
from tinybaker import Transform, fileset, sequence
from tinybaker.exceptions import (
    FileSetError,
    BakerError,
    BakerUnsupportedError,
    ConfigurationError,
)


def test_filesets_work_for_inputs():
    class Concat(Transform):
        input_tags = {fileset("files")}
        output_tags = {"concatted"}

        def script(self):
            content = ""
            for ref in self.input_files[fileset("files")]:
                with ref.open() as f:
                    content = content + f.read()

            with self.output_files["concatted"].open() as f:
                f.write(content)

    Concat(
        input_paths={
            fileset("files"): ["./tests/__data__/foo.txt", "./tests/__data__/bar.txt"],
        },
        output_paths={"concatted": "/tmp/concatted"},
        overwrite=True,
    ).run()

    with open("/tmp/concatted", "r") as f:
        assert f.read() == "foo contentsbar contents"


def test_filesets_work_for_outputs():
    class Concat(Transform):
        input_tags = {"source"}
        output_tags = {fileset("copied")}

        def script(self):
            with self.input_files["source"].open() as f:
                content = f.read()

            for ref in self.output_files[fileset("copied")]:
                with ref.open() as f:
                    f.write(content)

    outpaths = ["/tmp/copy1", "/tmp/copy2", "/tmp/copy3"]
    Concat(
        input_paths={
            "source": "./tests/__data__/foo.txt",
        },
        output_paths={fileset("copied"): outpaths},
        overwrite=True,
    ).run()

    for path in outpaths:
        with open(path, "r") as f:
            assert f.read() == "foo contents"


def test_filesets_dont_work_for_sequences():
    class One(Transform):
        input_tags = {"foo"}
        output_tags = {fileset("files")}

        def script(self):
            pass

    class Two(Transform):
        input_tags = {fileset("files")}
        output_tags = {"bar"}

        def script(self):
            pass

    with pytest.raises(BakerUnsupportedError):
        sequence([One, Two])(
            input_paths={"foo": "./tests/__data__/foo.txt"},
            output_paths={"bar": "./tests/__data__/bar.txt"},
            overwrite=True,
        ).run()


def test_filesets_dont_allow_single_files():
    class Blah(Transform):
        input_tags = {fileset("files")}
        output_tags = {"concatted"}

        def script(self):
            pass

    with pytest.raises(ConfigurationError):
        Blah(
            input_paths={
                fileset("files"): "/some/path",
            },
            output_paths={"concatted": "/tmp/concatted"},
            overwrite=True,
        )


def test_files_dont_allow_fileset():
    class Blah(Transform):
        input_tags = {"single_file"}
        output_tags = {"concatted"}

        def script(self):
            pass

    with pytest.raises(ConfigurationError):
        Blah(
            input_paths={
                "single_file": ["/some/path", "/some/other/path"],
            },
            output_paths={"concatted": "/tmp/concatted"},
            overwrite=True,
        )
