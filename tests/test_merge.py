from tinybaker import merge, StepDefinition


def test_merge():
    class StepOne(StepDefinition):
        input_file_set = {"foo"}
        output_file_set = {"bar"}

        def script(self):
            with self.input_files["foo"].open() as f:
                data = f.read()
            with self.output_files["bar"].open() as f:
                f.write(data + " processed")

    class StepTwo(StepDefinition):
        input_file_set = {"bloop"}
        output_file_set = {"bleep"}

        def script(self):
            with self.input_files["bloop"].open() as f:
                data = f.read()
            with self.output_files["bleep"].open() as f:
                f.write(data + " processed")

    Merged = merge([StepOne, StepTwo])

    Merged(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bloop": "./tests/__data__/bloop.txt",
        },
        output_paths={"bar": "/tmp/bar", "bleep": "/tmp/bleep"},
    ).build(overwrite=True)

    with open("/tmp/bar", "r") as f:
        assert f.read() == "foo contents processed"

    with open("/tmp/bleep", "r") as f:
        assert f.read() == "bloop contents processed"
