import os
import platform
from enum import Enum
from pathlib import Path

from epinorm.error import UserError


class DataSource(Enum):
    EMPRESI = "empresi"
    GENBANK = "genbank"
    ECDC = "ecdc"

    # Needed for argparse to display the string representation of the Enum.
    def __str__(self):
        return self.value

xdg_data_dir_by_os = {
    "Linux": (".local", "share"),
    "Darwin": ("Library", "Application Support"),
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
    
    # Make sure the specified directory exists.
    if not work_dir.exists() or not work_dir.is_dir():
        raise UserError(f"'XDG_DATA_HOME' directory '{work_dir}' does not exist")
    
    # If needed, create an "epinorm" subdirectory.
    work_dir /= "epinorm"
    if not work_dir.exists():
        try:
            Path.mkdir(work_dir, exist_ok=True)
        except (OSError, IOError):
            raise UserError(
                f"'XDG_DATA_HOME' directory '{work_dir}' appears to be "
                "non-writable"
            )

    return work_dir

# Directory paths.
PACKAGE_DIR = Path(__file__).parent
SCRIPT_DIR = PACKAGE_DIR / "scripts"
REF_DATA_DIR = PACKAGE_DIR / "data"
WORK_DIR = get_work_dir()
COUNTRIES_DATA = PACKAGE_DIR / "data" / "countries.csv"
ADMIN_LEVELS_DATA = PACKAGE_DIR / "data" / "administrative_units.tsv"

# ROOT_DIR = PACKAGE_DIR.parent
# DATA_DIR = ROOT_DIR / "data"
# INPUT_DIR = DATA_DIR / "input"
# OUTPUT_DIR = DATA_DIR / "output"
# AUX_DIR = OUTPUT_DIR / "auxiliaries"
# GEO_DIR = OUTPUT_DIR / "geometries"

# exceptions in data in the COUNTRIES_DATA file
COUNTRIES_EXCEPTIONS = {
    "Russia" : "RU",
    "Bolivia" : "BO",
    "Bonaire" : "BQ",
    "Bosnia" : "BA",
    "Iran" : "IR",
    "North Korea" : "KP",
    "North-Korea" : "KP",
    "South Korea" : "KR",
    "South-Korea" : "KR",
    "Moldova" : "MD",
    "Netherlands" : "NL",
    "Palestine" : "PS",
    "Taiwan" : "TW",
    "Tanzania" : "TZ",
    "United Kingdom" : "GB",
    "UK" : "GB",
    "United States" : "US",
    "US" : "US",
    "Venezuela" : "VE",
    "Vietnam" : "VN"
}

# these are countries that have a low admin level (for instance 3) but we don't want to use that for the
# output column "admin_level_1"
MIN_ADMIN_EXCEPTIONS = {
    "France" : 4,
    "China" : 4
}
