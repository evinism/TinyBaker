from tinybaker import map_tags, Transform
from tests.context import context


def test_map_tags():
    class Step(Transform):
        input_tags = {"bar"}
        output_tags = {"baz"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

    Mapped = map_tags(
        Step, input_mapping={"bar": "foo"}, output_mapping={"baz": "bloop"}
    )

    Mapped(
        input_paths={"foo": "./tests/__data__/foo.txt"},
        output_paths={"bloop": "/tmp/bloop"},
        context=context,
    ).build()

    with open("/tmp/bloop", "r") as f:
        assert f.read() == "foo contents processed"


def test_map_leaves_unreferenced_files_alone():
    class Step(Transform):
        input_tags = {"bar", "bleep"}
        output_tags = {"baz", "boppo"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

            with self.input_files["bleep"].open() as f:
                data = f.read()
            with self.output_files["boppo"].open() as f:
                f.write(data + " processed")

    Mapped = map_tags(
        Step, input_mapping={"bar": "foo"}, output_mapping={"baz": "bloop"}
    )

    Mapped(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bleep": "./tests/__data__/bleep.txt",
        },
        output_paths={"bloop": "/tmp/bloop", "boppo": "/tmp/boppo"},
        context=context,
    ).build()

    with open("/tmp/bloop", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "bleep contents processed"
