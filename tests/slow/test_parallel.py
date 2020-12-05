from multiprocessing import Process
from time import sleep
from tinybaker import Transform, BakerContext, ParallelMode, merge, map_tags
import pytest
import os


class AppendCat(Transform):
    input_tags = {"nocat"}
    output_tags = {"cat"}

    def script(self):
        with self.input_files["nocat"].open() as x:
            with self.output_files["cat"].open() as y:
                y.write(x.read() + " cat")


class AppendCatAlt(Transform):
    input_tags = {"nocatalt"}
    output_tags = {"catalt"}

    def script(self):
        with self.input_files["nocatalt"].open() as x:
            with self.output_files["catalt"].open() as y:
                y.write(x.read() + " cat")


@pytest.mark.skip("Combinators are non-pickle-able!")
def test_parallel_merge_combinators():
    for f in ["/tmp/out1", "/tmp/out2"]:
        if os.path.exists(f):
            os.remove(f)
    One = map_tags(AppendCat, {"nocat": "nocat1"}, {"cat": "cat1"})
    Two = map_tags(AppendCat, {"nocat": "nocat2"}, {"cat": "cat2"})
    Merged = merge([One, Two])
    Merged(
        {"nocat1": "./tests/__data__/foo.txt", "nocat2": "./tests/__data__/bar.txt"},
        {"cat1": "/tmp/out1", "cat2": "/tmp/out2"},
        context=BakerContext(parallel_mode=ParallelMode.MULTIPROCESSING),
    ).run()

    with open("/tmp/out1", "r") as f:
        assert f.read() == "foo contents cat"
    with open("/tmp/out2", "r") as f:
        assert f.read() == "bar contents cat"


@pytest.mark.skip("Run Infos are non-pickle-able!")
def test_parallel_merge():
    for f in ["/tmp/out1", "/tmp/out2"]:
        if os.path.exists(f):
            os.remove(f)

    Merged = merge([AppendCat, AppendCatAlt])
    Merged(
        {"nocat": "./tests/__data__/foo.txt", "nocatalt": "./tests/__data__/bar.txt"},
        {"cat": "/tmp/out1", "catalt": "/tmp/out1"},
        context=BakerContext(parallel_mode=ParallelMode.MULTIPROCESSING),
    ).run()

    with open("/tmp/out1", "r") as f:
        assert f.read() == "foo contents cat"
    with open("/tmp/out2", "r") as f:
        assert f.read() == "bar contents cat"
