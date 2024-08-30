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

HOST_SPECIES_FILE = REF_DATA_DIR / "ncbi_host_species.csv"
PATHOGEN_SPECIES_FILE = REF_DATA_DIR / "ncbi_pathogen_species.csv"

COUNTRIES_FILE = REF_DATA_DIR / "countries.csv"
ADMIN_UNITS_FILE = REF_DATA_DIR / "administrative_units.tsv"
ADMIN_LEVEL_1_FILE = REF_DATA_DIR / "admin_level_1.csv"
NUTS_2024_FILE = REF_DATA_DIR / "nuts_2024.csv"
NUTS_COORDINATES_FILE = REF_DATA_DIR / "NUTS_LB_2021_4326.geojson"

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
    "Plurinational State of Bolivia" : "BO",
    "Bonaire" : "BQ",
    "Bosnia" : "BA",
    "Iran" : "IR",
    "Islamic Republic of Iran" : "IR",
    "North Korea" : "KP",
    "North-Korea" : "KP",
    "Democratic People's Republic of Korea" : "KP",
    "South Korea" : "KR",
    "South-Korea" : "KR",
    "Republic of Korea" : "KR",
    "Moldova" : "MD",
    "Republic of Moldova": "MD",
    "Netherlands" : "NL",
    "Kingdom of the Netherlands" : "NL",
    "Micronesia": "FM",
    "Federated States of Micronesia": "FM",
    "Palestine" : "PS",
    "State of Palestine" : "PS",
    "Taiwan" : "TW",
    "Province of China Taiwan" : "TW",
    "Tanzania" : "TZ",
    "United Republic of Tanzania" : "TZ",
    "United Kingdom" : "GB",
    "UK" : "GB",
    "United States" : "US",
    "US" : "US",
    "Venezuela" : "VE",
    "Vietnam" : "VN",
    "Democratic Republic of the Congo" : "CG",
}

# some countries don't use their contry code in their nuts code
# for instance greece with country code GR uses EL as country code
NUTS_CODE_TO_COUNTRY_EXCEPTIONS = {
    "EL" : "GR"
}

# there are hardcoded exceptions for the "admin_level_1.csv" file
ADMIN_LEVEL_1_EXCEPTIONS = {

    # there were too many entries in the datasets with nuts level 1.
    # if we had chosen a lower level then we would drop too many rows
    "FR" : {"nuts_level": 1, "osm_level": 4},

    # the nuts level 3 divide the nuts level 2 into very few areas, that don't
    # have a osm_id. The administrative units within 2 are too small compared to the nuts level 3
    "NL" : {"nuts_level": 2, "osm_level": 4},
    "DK" : {"nuts_level": 2, "osm_level": 4},
    "AT" : {"nuts_level": 2, "osm_level": 4},
    "PL" : {"nuts_level": 2, "osm_level": 4},

    # these countries are very small, their nuts codes doesn't contain precise information.
    # we remove their nuts level and only keep osm_level
    "CY" : {"nuts_level": 4, "osm_level": 3},
    "MT" : {"nuts_level": 4, "osm_level": 4},
    "LU" : {"nuts_level": 4, "osm_level": 6},

    # osm doesn't yet contain the nuts codes for those countries
    "PT" : {"nuts_level": 3, "osm_level": 6},
    "EE" : {"nuts_level": 3, "osm_level": 6},
    "IE" : {"nuts_level": 3, "osm_level": 5},
    "GR" : {"nuts_level": 3, "osm_level": 6},
    "LV" : {"nuts_level": 3, "osm_level": 4},

    # China has 3 as min admin level, but we prefer 4
    "CN" : {"nuts_level": None, "osm_level": 4},
}
