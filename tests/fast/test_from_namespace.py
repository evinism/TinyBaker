import ns_transform as ns
from tinybaker import Transform


def test_from_namespace():
    Tform = Transform.from_namespace(ns)
    job = Tform(
        {"infile": "./tests/__data__/foo.txt"},
        {"outfile": "/tmp/tbtests_zeep.txt"},
        overwrite=True,
    )

    job.run()

    with open("/tmp/tbtests_zeep.txt") as f:
        assert f.read() == "foo contents but different"
