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

    Mapped = map_tags(Step, input_mapping={"foo": "bar"}, output_mapping={"baz": "bloop"})

    Mapped(
        input_paths={"foo": "./tests/__data__/foo.txt"}, output_paths={"bloop": "/tmp/bloop"}
    ).build(overwrite=True)

    with open("/tmp/bloop", "r") as f:
        assert f.read() == "foo contents processed"