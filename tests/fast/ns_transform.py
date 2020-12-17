from tinybaker import InputTag, OutputTag, cli

infile = InputTag("infile")
outfile = OutputTag("outfile")


def script():
    with infile.open() as f:
        contents = f.read()

    with outfile.open() as f:
        f.write(contents + " but different")


if __name__ == "__main__":
    cli(locals())