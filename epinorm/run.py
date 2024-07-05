import logging
from typing import Callable

from epinorm.cli import cli_argument_parser


def main() -> None:
    """Main entry point of EpiNorm application."""
    
    

    args = vars(cli_argument_parser())
    subcmd_function: Callable = args.pop("func")
    show_debug_logs: bool = args.pop("debug")

    # Configure logging.
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if show_debug_logs else logging.INFO,
    )
    
    # Run the selected subcommand.
    print(args)
    subcmd_function(**args)


if __name__ == "__main__":
    main()