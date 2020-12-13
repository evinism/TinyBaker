from tinybaker import Transform, sequence, merge, map_tags


def test_basic_structure():
    class Foo(Transform):
        input_tags = {"hi", "there"}
        output_tags = {"oh", "hey"}

        def script():
            pass

    assert Foo.structure() == {
        "type": "leaf",
        "name": "Foo",
        "input_tags": ["hi", "there"],
        "output_tags": ["hey", "oh"],
    }


def test_basic_sequence():
    class Foo(Transform):
        input_tags = {"yo"}
        output_tags = {"hi"}

        def script():
            pass

    class Bar(Transform):
        input_tags = {"hi", "there"}
        output_tags = {"sup", "dude"}

        def script():
            pass

    assert sequence([Foo, Bar], name="Baz").structure() == {
        "type": "sequence",
        "name": "Baz",
        "input_tags": ["there", "yo"],
        "output_tags": ["dude", "sup"],
        "steps": [
            {
                "input_tags": ["yo"],
                "name": "Foo",
                "output_tags": ["hi"],
                "type": "leaf",
            },
            {
                "input_tags": ["hi", "there"],
                "name": "Bar",
                "output_tags": ["dude", "sup"],
                "type": "leaf",
            },
        ],
    }


def test_map():
    class Bar(Transform):
        input_tags = {"hi", "there"}
        output_tags = {"sup", "dude"}

        def script():
            pass

    Mapped = map_tags(
        Bar, {"hi": "hey", "there": "thar"}, {"sup": "suh", "dude": "bruh"}
    )

    assert Mapped.structure() == {
        "type": "map",
        "name": "Bar",
        "input_tags": ["hey", "thar"],
        "output_tags": ["bruh", "suh"],
        "base_step": {
            "type": "leaf",
            "name": "Bar",
            "input_tags": ["hi", "there"],
            "output_tags": ["dude", "sup"],
        },
    }


def test_merge():
    class Foo(Transform):
        input_tags = {"hi"}
        output_tags = {"there"}

        def script():
            pass

    class Bar(Transform):
        input_tags = {"sup"}
        output_tags = {"dude"}

        def script():
            pass

    assert merge([Foo, Bar], name="Baz").structure() == {
        "type": "merge",
        "name": "Baz",
        "input_tags": ["hi", "sup"],
        "output_tags": ["dude", "there"],
        "steps": [
            {
                "input_tags": ["hi"],
                "name": "Foo",
                "output_tags": ["there"],
                "type": "leaf",
            },
            {
                "input_tags": ["sup"],
                "name": "Bar",
                "output_tags": ["dude"],
                "type": "leaf",
            },
        ],
    }