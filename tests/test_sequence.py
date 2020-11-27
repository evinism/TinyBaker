from tinybaker import sequence, StepDefinition

def test_sequence():
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
                f.write(data + " processed")

    Seq = sequence([StepOne, StepTwo])

    Seq(
        input_paths={"foo": "./tests/__data__/foo.txt"}, output_paths={"baz": "/tmp/baz"}).build(
        overwrite=True
    )

    with open("/tmp/baz", "r") as f:
        assert f.read() == "foo contents processed"
