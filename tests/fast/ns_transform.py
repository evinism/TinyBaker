from tinybaker import InputTag, OutputTag, cli

infile = InputTag("infile")
outfile = OutputTag("outfile")


def script():
    with infile.ref.open() as f:
        contents = f.read()

    with outfile.ref.open() as f:
        f.write(contents + " but different")


if __name__ == "__main__":
    cli(locals())