import os
import platform
import logging
from pathlib import Path

# Directory paths
PACKAGE_DIR = Path(__file__).parent
SCRIPT_DIR = PACKAGE_DIR / "scripts"
REF_DATA_DIR = PACKAGE_DIR / "data"
ROOT_DIR = PACKAGE_DIR.parent
DATA_DIR = ROOT_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
AUX_DIR = OUTPUT_DIR / "auxiliaries"
GEO_DIR = OUTPUT_DIR / "geometries"

xdg_data_dir_by_os = {
    "Linux": (".local", "share"),
    "Darwin": (".local", "share"),
    "Windows": ("AppData", "Roaming"),
}

def get_work_dir() -> Path:
    """
    Returns the path of the "work directory" used by epinorm.
    """
    
    # Test whether a custom "data dir" is specified by the user, otherwise
    # use the platform specific location for the current OS.
    if xdg_data_dir := os.environ.get('XDG_DATA_HOME'):
        work_dir = Path(xdg_data_dir).expanduser()
    else:
        work_dir = Path.home().joinpath(*xdg_data_dir_by_os[platform.system()])
    
    # Make sure the specified directory exits.
    if not work_dir.exists() or not work_dir.is_dir():
        logging.error(f"'XDG_DATA_HOME' directory '{work_dir}' does not exist")
    
    # If needed, create an "epinorm" subdirectory.
    work_dir /= "epinorm"
    if not work_dir.exists():
        Path.mkdir(work_dir, exist_ok=True)

    return work_dir

WORK_DIR = get_work_dir()
