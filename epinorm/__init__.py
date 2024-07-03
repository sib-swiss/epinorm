from pathlib import Path

# Directory paths
PACKAGE_DIR = Path(__file__).parent
SCRIPT_DIR = PACKAGE_DIR / "scripts"
REF_DATA_DIR = PACKAGE_DIR / "data"
ROOT_DIR = PACKAGE_DIR.parent
DATA_DIR = ROOT_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
WORK_DIR = DATA_DIR / "work"
AUX_DIR = OUTPUT_DIR / "auxiliaries"
GEO_DIR = OUTPUT_DIR / "geometries"
