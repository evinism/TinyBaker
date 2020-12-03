import pytest
from tinybaker import Transform, fileset
from tinybaker.exceptions import FileSetError, BakerError


def test_filesets_mostly_work():
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
            fileset("files"): {"./tests/__data__/foo.txt", "./tests/__data__/bar.txt"},
        },
        output_paths={"concatted": "/tmp/concatted"},
        overwrite=True,
    ).build()

    with open("/tmp/concatted", "r") as f:
        assert f.read() == "bar contentsfoo contents"
