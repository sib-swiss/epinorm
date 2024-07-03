"""CLI builder for EpiNorm."""

import argparse
from enum import Enum

class DataSource(Enum):
    EMPRESI = "empresi"
    GENBANK = "genbank"
    ECDC = "ecdc"


def run_normalize(args):
    print("Starting the normalization workflow...")
    print("Input args:", args)

def run_merge(args):
    print("Starting the file merge workflow...")
    print("Input args:", args)

def run_clear_cache(args):
    print("Starting the clear-cache workflow...")
    print("Input args:", args)

def cli_argument_parser() -> argparse.Namespace:
    """Setup the CLI arguments.

    epinorm normalize -s empresi input.csv

    -s/--data-source, one of DataSources.
    -f/--output-file optional output file name.
    -d/--output-dir optional output dir. By default, an "output" directory is created in the cwd
    -a/--write-auxiliaries

    epinorm merge -d
    epinorm clear-cache
    """

    parser = argparse.ArgumentParser(
        description="EpiNorm - the MOOD data standardization tool",
        prog="epinorm"
    )
    subparser = parser.add_subparsers(title="EpiNorm subcommands", required=True)

    subcmd_normalize = subparser.add_parser("normalize", help="Normalize input data")
    subcmd_normalize.add_argument(
        "-s",
        "--data-source",
        type=str,
        choices=[x.value for x in DataSource],
        help="Input data source file to process."
    )
    subcmd_normalize.add_argument("-f", "--output-file", type=str, help="Output file.")
    subcmd_normalize.add_argument("-d", "--output-dir", type=str, help="Output directory.")
    subcmd_normalize.add_argument(
        "-a",
        "--write-auxiliaries",
        action="store_true",
        default=False,
        help="Write auxillary files. Default: %(default)s."
    )
    subcmd_normalize.set_defaults(func=run_normalize)

    subcmd_merge = subparser.add_parser("merge", help="Merge files")
    subcmd_merge.set_defaults(func=run_merge)

    subcmd_clear = subparser.add_parser("clear-cache", help="Clear cache")
    subcmd_clear.set_defaults(func=run_clear_cache)

    # parser.add_argument(
    #     "-v",
    #     "--verbose",
    #     dest="log_level",
    #     const=logging.DEBUG,
    #     default=logging.INFO,
    #     help="increase verbosity",
    #     action="store_const",
    # )
    return parser.parse_args()


def main():
    """Main entry point of EpiNorm application."""
    args = cli_argument_parser()
    args.func(args)



if __name__ == "__main__":
    main()
