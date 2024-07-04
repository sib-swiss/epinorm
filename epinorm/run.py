import logging

from epinorm.cli import cli_argument_parser


def main():
    """Main entry point of EpiNorm application."""
    
    # Configure logging.
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    args = vars(cli_argument_parser())
    subcmd_function = args.pop("func")
    print(args)
    subcmd_function(**args)


if __name__ == "__main__":
    main()