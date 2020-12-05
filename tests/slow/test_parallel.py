from multiprocessing import Process
from time import sleep
from tinybaker import Transform, BakerContext, ParallelMode, merge, map_tags
import os


class AppendCat(Transform):
    input_tags = {"nocat"}
    output_tags = {"cat"}

    def script(self):
        sleep(0.2)
        with self.input_files["nocat"].open() as x:
            with self.output_files["cat"].open() as y:
                y.write(x.read() + " cat")


def append_cat(in_file, out_file):
    AppendCat(
        {
            "nocat": in_file,
        },
        {
            "cat": out_file,
        },
        overwrite=True,
    ).run()


def test_two_steps():
    for f in ["/tmp/out1", "/tmp/out2"]:
        if os.path.exists(f):
            os.remove(f)
    p1 = Process(target=append_cat, args=("./tests/__data__/foo.txt", "/tmp/out1"))
    p2 = Process(target=append_cat, args=("./tests/__data__/bar.txt", "/tmp/out2"))
    p1.start()
    p2.start()
    p1.join()
    p2.join()

    with open("/tmp/out1", "r") as f:
        assert f.read() == "foo contents cat"
    with open("/tmp/out2", "r") as f:
        assert f.read() == "bar contents cat"


def test_parallel_merge():
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
