from typing import Dict, Any
import argparse
from time import time
from .transform import Transform, TransformMeta, TransformCoercable, coerce_to_transform


def _build_parser(TF: TransformMeta):
    parser = argparse.ArgumentParser(
        description="Execute a {} transform".format(TF.name)
    )
    for tag in TF.input_tags:
        parser.add_argument(
            "--{}".format(tag),
            dest=tag,
            action="store",
            type=str,
            required=True,
            help="Path for output tag {}".format(tag),
        )

    for tag in TF.output_tags:
        parser.add_argument(
            "--{}".format(tag),
            dest=tag,
            action="store",
            type=str,
            required=True,
            help="Path for output tag {}".format(tag),
        )

    parser.add_argument("--version", action="version", version="%(prog)s 0.2.5")
    parser.add_argument(
        "--overwrite",
        dest="__overwrite__",
        action="store_true",
        help="Whether to overwrite any existing output files",
    )
    return parser


def cli(source: TransformCoercable):
    """
    Runs a CLI for the specified transform. Can take any transform-coercable data structure.

    :param source: The transform-coercible object to build a CLI around
    """
    TF = coerce_to_transform(source)

    parser = _build_parser(TF)
    args = parser.parse_args()

    input_paths = {tag: getattr(args, tag) for tag in TF.input_tags}
    output_paths = {tag: getattr(args, tag) for tag in TF.output_tags}

    job = TF(input_paths, output_paths, overwrite=args.__overwrite__)

    print("[ TinyBaker ]")
    print("Running job for {} with following configuration:".format(TF.name))
    print("  Inputs:")
    for tag in input_paths:
        path = input_paths[tag]
        print("    {}\t\t{}".format(tag, path))

    print("  Outputs:")
    for tag in output_paths:
        path = output_paths[tag]
        print("    {}\t\t{}".format(tag, path))
    start = time()

    job.run()

    end = time()
    seconds = round((end - start) * 1000) / 1000
    print("Finished! (took {} seconds)".format(seconds))