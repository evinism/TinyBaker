import tests.__data__.ns_transform as ns
from tinybaker import namespace_to_transform


def test_from_namespace():
    transform = namespace_to_transform(ns)
