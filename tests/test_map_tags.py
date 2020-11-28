from tinybaker import map_tags, StepDefinition


def test_map_tags():
    class Step(StepDefinition):
        input_file_set = {"bar"}
        output_file_set = {"baz"}

        def script(self):
            with self.input_files["bar"].open() as f:
                data = f.read()
            with self.output_files["baz"].open() as f:
                f.write(data + " processed")

    Mapped = map_tags(
        Step, input_mapping={"foo": "bar"}, output_mapping={"baz": "bloop"}
    )

    Mapped(
        input_paths={"foo": "./tests/__data__/foo.txt"},
        output_paths={"bloop": "/tmp/bloop"},
    ).build(overwrite=True)

    with open("/tmp/bloop", "r") as f:
        assert f.read() == "foo contents processed"


def test_map_leaves_unreferenced_files_alone():
    class Step(StepDefinition):
        input_file_set = {"bar", "bleep"}
        output_file_set = {"baz", "boppo"}

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
        Step, input_mapping={"foo": "bar"}, output_mapping={"baz": "bloop"}
    )

    Mapped(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bleep": "./tests/__data__/bleep.txt",
        },
        output_paths={"bloop": "/tmp/bloop", "boppo": "/tmp/boppo"},
    ).build(overwrite=True)

    with open("/tmp/bloop", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "bleep contents processed"
