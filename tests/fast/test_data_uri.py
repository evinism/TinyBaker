from tinybaker import Transform, InputTag, OutputTag
from pickle import load, dumps
from base64 import b64encode


def test_data_uri_text():
    class T(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            with self.foo.open() as f:
                data = f.read()
            with self.bar.open() as f:
                f.write(data)

    bar_path = "/tmp/datauri"
    T(
        input_paths={"foo": "data://Zm9vIGNvbnRlbnRz"},
        output_paths={"bar": bar_path},
        overwrite=True,
    ).run()
    with open(bar_path, "r") as f:
        assert f.read() == "foo contents"


def test_data_uri_utf8():
    class T(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            with self.foo.open() as f:
                data = f.read()
            with self.bar.open() as f:
                f.write(data)

    bar_path = "/tmp/datauri2"
    T(
        input_paths={"foo": "data://2YjZitmD2YrYqNmK2K/ZitinINin2YTYudix2KjZitip"},
        output_paths={"bar": bar_path},
        overwrite=True,
    ).run()
    with open(bar_path, "r") as f:
        assert f.read() == "ويكيبيديا العربية"


def test_data_binary():
    class T(Transform):
        foo = InputTag("foo")
        bar = OutputTag("bar")

        def script(self):
            with self.foo.openbin() as f:
                data = load(f)
            assert data == {"hi": str, "bye": [int]}
            with self.bar.open() as f:
                f.write("success")

    bar_path = "/tmp/datauri"
    obj = {"hi": str, "bye": [int]}

    uri_data = b64encode(dumps(obj))
    T(
        input_paths={"foo": "data://{}".format(uri_data.decode("ascii"))},
        output_paths={"bar": "/dev/null"},
        overwrite=True,
    ).run()
