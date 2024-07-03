import logging

from epinorm import (
    INPUT_DIR,
    OUTPUT_DIR,
)
#from epinorm.cache import SQLiteCache
from epinorm.norm import EmpresiDataHandler

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

# Define input and output files
input_file = INPUT_DIR / "empresi" / "ai_sample.csv"
output_file = OUTPUT_DIR / "empresi" / "ai_norm.tsv"

# Define geometry output directory
geometry_dir = OUTPUT_DIR / "empresi" / "geometries"

# Clear the cache database
#SQLiteCache.delete_db()

# Normalize the data
data_handler = EmpresiDataHandler(input_file)
sampled_data = data_handler.sample_rows("top", 10)
data_handler.set_data(sampled_data)
data_handler.normalize()

# Save the normalized data and geometries
data_handler.save_data(output_file)
data_handler.save_geometries(geometry_dir)
