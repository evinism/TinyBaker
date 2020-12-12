from tinybaker import Transform, map_tags, sequence, merge


def test_basic_name():
    class Lol(Transform):
        def script(self):
            pass

    assert Lol.name == "Lol"


def test_specified_basic_name():
    class Lol(Transform):
        name = "NotLol"

        def script(self):
            pass

    assert Lol.name == "NotLol"


def test_sequence_generated_name():
    class Lol(Transform):
        def script(self):
            pass

    assert sequence([Lol, Lol]).name == "Seq(Lol,Lol)"


def test_sequence_defined_name():
    class Lol(Transform):
        def script(self):
            pass

    assert sequence([Lol, Lol], name="TootToot").name == "TootToot"


def test_merge_generated_name():
    class Lol(Transform):
        def script(self):
            pass

    assert merge([Lol, Lol]).name == "Merge(Lol,Lol)"


def test_merge_defined_name():
    class Lol(Transform):
        def script(self):
            pass

    assert merge([Lol, Lol], name="TootToot").name == "TootToot"


def test_map_generated_name():
    class Lol(Transform):
        def script(self):
            pass

    assert map_tags(Lol).name == "Lol"


def test_map_defined_name():
    class Lol(Transform):
        def script(self):
            pass

    assert map_tags(Lol, name="TootToot").name == "TootToot"