import os
import logging
import platform
import random
from pathlib import Path

from epinorm.norm import EmpresiDataHandler, GenBankDataHandler, ECDCDataHandler
from epinorm.cache import SQLiteCache, DB_FILE
from epinorm.config import DataSource
from epinorm.error import UserError


class ValidatedArgs:
    """Class to validate and hold user inputs."""

    def __init__(
        self,
        data_source: DataSource,
        input_file: Path,
        output_dir: Path | None = None,
        output_file_name: str | None = None
    ):
        # Verify the input file exists.
        if not input_file.exists() or not input_file.is_file():
            raise UserError(
                f"Input file '{input_file}' does not exist "
                "or is not a regular file."
            )

        # Verify the output directory exists and is writable.
        if not output_dir:
            output_dir = Path.cwd()
        if not output_dir.exists() or not output_dir.is_dir():
            raise UserError(
                f"Output directory '{output_dir}' does not exist "
                "or is not a directory."
            )
        verify_dir_is_writable(output_dir)

        # Create class attributes.
        self._input_file = input_file
        self._output_dir = output_dir / "epinorm_output" / data_source.value
        # If needed, derive the output file name from input file.
        if not output_file_name:
            output_file_name = f"{input_file.stem}_normalized{input_file.suffix}"
        self._output_file = self._output_dir / output_file_name

    @property
    def input_file(self) -> Path:
        return self._input_file
    
    @property
    def output_file(self) -> Path:
        return self._output_file

    @property
    def output_dir(self) -> Path:
        return self._output_dir

    @property
    def auxiliaries_dir(self) -> Path:
        return self._output_dir / "auxiliaries"

    @property
    def geometries_dir(self) -> Path:
        return self._output_dir / "geometries"
    

def normalize_data(
    data_source: DataSource,
    input_file: Path,
    output_dir: Path | None = None, 
    output_file_name: str | None = None,
    write_auxiliaries: bool = False,
    dry_run: bool = False,
):
    """Main epinorm workflow."""

    logging.info("Starting the normalization workflow...")
    
    # Verify user inputs.
    args = ValidatedArgs(data_source, input_file, output_dir, output_file_name)

    # Normalize data according to output file
    if (data_source == DataSource.EMPRESI):
        data_handler = EmpresiDataHandler(input_file)
    elif (data_source == DataSource.GENBANK):
        data_handler = GenBankDataHandler(input_file)
    else:
        data_handler = ECDCDataHandler(input_file)

    if dry_run:
        logging.info("Running in --dry-run mode")
        sampled_data = data_handler.sample_rows("top", 10)
        data_handler.set_data(sampled_data)    
        
    data_handler.normalize()

    # Save the normalized data and geometries.
    if not args.output_dir.exists():
        Path.mkdir(args.output_dir, parents=True)
    logging.debug(f"Writing normalized data to {args.output_file}")
    data_handler.save_data(args.output_file)
    logging.debug(f"Writing geometries to {args.geometries_dir}")
    data_handler.save_geometries(args.geometries_dir)
    
    # Display end-of-task message.
    logging.info("Data normalization completed.")
    logging.info(f"Normalized data saved to: {args.output_file}")
    logging.info(f"Geometries saved to: {args.geometries_dir}")


def merge_data(args):
    print("Starting the file merge workflow...")
    print("Input args:", args)

def clear_cache():
    logging.info(f"Deleting epinorm cache database at '{DB_FILE}'")
    SQLiteCache.delete_db()
    logging.info("Deletion completed")


def verify_dir_is_writable(dir: Path) -> None:
    """Verify that the specified `dir` directory has write permission for
    the current user.
    """
    if platform.system() in ("Linux", "Darwin"):
        is_writable = dir_is_writable_unix(dir)
    else:
        is_writable = dir_is_writable_windows(dir)
    
    if not is_writable:
        raise UserError(f"output directory {dir} appears to be non-writable.")
        

def dir_is_writable_unix(dir: Path) -> bool:
    return os.access(dir, mode=os.W_OK)

def dir_is_writable_windows(dir: Path) -> bool:
    # Notes:
    #  * Using "os.access('dir', os.W_OK)" does apparently not work on Windows.
    #  * Using the tempfile module for creating a temporary file does also
    #    seem to create issues on windows. Therefore we generate our own
    #    random file and then delete it.
    # * `IOError` is apparently returned on Windows when a dir is non-writable.
    tmp_file = dir / ("test_file_" + "".join(random.choices('0123456789', k=7)))
    try:
        with open(tmp_file, mode="w") as _:
            pass
        tmp_file.unlink()
        return True
    except (OSError, IOError):
        return False

# @dataclass(frozen=True)
# class Config:
#     data_source: DataSource
#     input_file: Path
#     output_root_dir: Path = Path.home()
#     output_file_name: str | None = None

#     def __post_init__(self):
#         # Verify the input file exists.
#         if not self.input_file.exists() or not self.input_file.is_file():
#             logging.error(
#                 f"Input file '{self.input_file}' does not exist "
#                 "or is not a regular file."
#             )

#         # Verify the output directory exists and is writable.
#         if not self.output_root_dir.exists() or not self.output_root_dir.is_dir():
#             logging.error(
#                 f"Output directory '{self.output_root_dir}' does not exist "
#                 "or is not a directory."
#             )
#         verify_dir_is_writable(self.output_root_dir)

#     @property
#     def output_dir(self) -> Path:
#         return self.output_root_dir / "epinorm_output" / self.data_source.value

#     @property
#     def output_file(self) -> Path:
#         if self.output_file_name:
#             return self.output_dir / self.output_file_name
#         # Derive output name from input file name.
#         input_file_path = Path(self.input_file)
#         return self.output_dir / f"{input_file_path.stem}_normalized{input_file_path.suffix}"

#     @property
#     def auxiliaries_dir(self) -> Path:
#         return self.output_dir / "auxiliaries"

#     @property
#     def geometries_dir(self) -> Path:
#         return self.output_dir / "geometries"
    
