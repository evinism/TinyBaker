from tinybaker import InputTag, OutputTag

infile = InputTag("infile")
outfile = OutputTag("outfile")


def script():
    with infile.ref.open() as f:
        contents = f.read()

    with outfile.ref.open() as f:
        f.write(contents + " but different")