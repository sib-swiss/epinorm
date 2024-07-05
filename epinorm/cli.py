"""CLI builder for EpiNorm."""

import argparse
from pathlib import Path

from epinorm.workflows import DataSource, normalize_data, merge_data, clear_cache

def str_to_datasource(value: str) -> DataSource:
    try:
        return DataSource(value)
    except argparse.ArgumentTypeError:
        raise argparse.ArgumentTypeError("bad value...")
    
    return DataSource(value)


def cli_argument_parser() -> argparse.Namespace:
    """Setup the CLI arguments.

    epinorm normalize -s empresi input.csv

    -s/--data-source, one of DataSources.
    -f/--output-file optional output file name.
    -d/--output-dir optional output dir. By default, an "output" directory is created in the cwd
    -a/--write-auxiliaries
    --dry-run
    --debug


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
        type=DataSource,
        choices=list(DataSource),
        help="Input data source file to process."
    )
    subcmd_normalize.add_argument("-f", "--output-file-name", type=str, help="Output file.")
    subcmd_normalize.add_argument("-d", "--output-dir", type=Path, help="Output directory.")
    subcmd_normalize.add_argument(
        "-a",
        "--write-auxiliaries",
        action="store_true",
        help="Write auxillary files. Default: %(default)s."
    )
    subcmd_normalize.add_argument(
        "--dry-run",
        action='store_true',
        help="Test run on a subset of 10 values"
    )
    subcmd_normalize.add_argument(
        "--debug",
        action='store_true',
        help="Display debug information messages"
    )
    subcmd_normalize.add_argument("input_file", type=Path)
    subcmd_normalize.set_defaults(func=normalize_data)

    subcmd_merge = subparser.add_parser("merge", help="Merge files")
    subcmd_merge.set_defaults(func=merge_data)

    subcmd_clear = subparser.add_parser("clear-cache", help="Clear cache")
    subcmd_clear.set_defaults(func=clear_cache)

    return parser.parse_args()
