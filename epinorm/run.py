import logging
from typing import Callable

from epinorm.cli import cli_argument_parser


def main() -> None:
    """Main entry point of EpiNorm application."""
    
    # Retrieve user input from the command line.
    args = vars(cli_argument_parser())

    # Configure logging.
    show_debug_logs: bool = args.pop("debug") if "debug" in args else False
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if show_debug_logs else logging.INFO,
    )
    
    # Run the selected subcommand.
    subcmd_function: Callable = args.pop("func")
    subcmd_function(**args)


if __name__ == "__main__":
    main()