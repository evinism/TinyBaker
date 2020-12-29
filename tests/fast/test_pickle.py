from tinybaker import Transform, InputTag, OutputTag, sequence
import pickle


class StepOne(Transform):
    foo = InputTag("foo")
    bar = OutputTag("bar")

    def script(self):
        with self.foo.open() as f:
            data = f.read()
        with self.bar.open() as f:
            f.write(data)


class StepTwo(Transform):
    bar = InputTag("bar")
    baz = OutputTag("baz")

    def script(self):
        with self.bar.open() as f:
            data = f.read()
        with self.baz.open() as f:
            f.write(data + " processed")


class StepThree(Transform):
    baz = InputTag("baz")
    bleep = InputTag("bleep")
    boppo = OutputTag("boppo")

    def script(self):
        with self.baz.open() as f:
            data = f.read()
        with self.bleep.open() as f:
            data2 = f.read()
        with self.boppo.open() as f:
            f.write(data + " " + data2)


BaseSeq = sequence([StepOne, sequence([StepTwo, StepThree])])


def test_pickle_nested_sequence():
    Seq = pickle.loads(pickle.dumps(BaseSeq))

    Seq(
        input_paths={
            "foo": "./tests/__data__/foo.txt",
            "bleep": "./tests/__data__/bleep.txt",
        },
        output_paths={"boppo": "/tmp/boppo"},
        overwrite=True,
    ).run()

    with open("/tmp/boppo", "r") as f:
        assert f.read() == "foo contents processed bleep contents"
