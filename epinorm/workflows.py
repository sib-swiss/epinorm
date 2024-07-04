import logging
from enum import Enum
from pathlib import Path
from typing import Optional

from epinorm.norm import EmpresiDataHandler
from epinorm.cache import SQLiteCache

class DataSource(Enum):
    EMPRESI = "empresi"
    GENBANK = "genbank"
    ECDC = "ecdc"

    # Needed for argparse to display the string representation of the Enum.
    def __str__(self):
        return self.value

def normalize_data(
    data_source: DataSource,
    input_file: str,
    output_file: str,
    output_dir: Optional[Path], 
    write_auxiliaries: bool = False,
    dry_run: bool = False,
):
    print("Starting the normalization workflow...")
    
    # Normalize data.
    data_handler = EmpresiDataHandler(input_file)
    if dry_run:
        logging.info("Running in --dry-run mode")
        sampled_data = data_handler.sample_rows("top", 10)
        data_handler.set_data(sampled_data)
    
    data_handler.normalize()

    # Create output directory structure.
    if output_dir is None:
        output_dir = Path.cwd()
    if output_file is None:
        input_file_path = Path(input_file)
        output_file = f"{input_file_path.stem}_normalized{input_file_path.suffix}"

    output_dir = output_dir / "epinorm_output" / data_source.value
    output_file_path = output_dir / output_file
    geometry_dir = output_dir / "geometries"
    Path.mkdir(output_dir, parents=True, exist_ok=True)

    # Save the normalized data and geometries.
    logging.info("Writing outputs to {output_dir}")
    logging.debug("Writing normalized data to {output_file_path}")
    logging.debug("Writing geometries to {geometry_dir}")
    data_handler.save_data(output_file_path)
    data_handler.save_geometries(geometry_dir)
    
    print("Data normalization completed:")
    print(" -> Normalized data saved to:", output_file_path)
    print(" -> Geometries saved to:", output_file_path)


def merge_data(args):
    print("Starting the file merge workflow...")
    print("Input args:", args)

def clear_cache(args):
    print("Starting the clear-cache workflow...")
    print("Input args:", args)
    
    SQLiteCache.delete_db()