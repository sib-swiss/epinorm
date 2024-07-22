import logging
import pandas as pd
import requests
import time

from csv import QUOTE_NONE
from io import StringIO
from epinorm import (
    DATA_DIR,
    REF_DATA_DIR,
)


# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
INPUT_FILE = REF_DATA_DIR / "countries.csv"
OUTPUT_FILE = REF_DATA_DIR / "administrative_units.tsv"
CACHE_DIR = DATA_DIR / "work" / "admin_units"
ADMIN_LEVELS = [3, 4, 5, 6]
FIELD_SEPARATOR = "\t"
DATA_COLUMNS = [
    "iso3166_1_code",
    "iso3166_2_code",
    "exonym",
    "endonym",
    "code",
    "nuts_code",
    "wikidata_id",
    "osm_id",
    "admin_level",
]


def rename_columns(data):
    """Rename columns to match the schema."""
    return data.rename(
        columns={
            "name": "endonym",
            "name:en": "exonym",
            "admin_level": "admin_level",
            "wikidata": "wikidata_id",
            "ref": "code",
            "ref:nuts": "nuts_code",
            "ISO3166-2": "iso3166_2_code",
            "ISO3166-1": "iso3166_1_code",
        }
    )


def add_missing_columns(data):
    """Add missing columns to match the schema."""
    missing_columns = set(DATA_COLUMNS) - set(data.columns)
    for column in missing_columns:
        data[column] = None
    return data


def reorder_columns(data):
    """Reorder columns to match the schema."""
    return data[DATA_COLUMNS]


def normalize_data(data):
    """Normalize the data."""
    if data["exonym"].dtype == "O":
        data["exonym"] = data["exonym"].str.replace("–", "-")
    if data["endonym"].dtype == "O":
        data["endonym"] = data["endonym"].str.replace("–", "-")
    return data


def filter_rows(data):
    """Remove administrative areas that do not belong to the current country."""
    return data[
        data.apply(
            lambda row: (
                str(row["iso3166_2_code"]).startswith(row["iso3166_1_code"])
                if pd.notna(row["iso3166_2_code"])
                else True
            ),
            axis=1,
        )
    ]


def sort_rows(data):
    """Sort rows by country code, admin level, exonym, and endonym."""
    return data.sort_values(
        by=["iso3166_1_code", "admin_level", "exonym", "endonym"],
        ignore_index=True,
    )


def save_data(data, filename):
    """Save the data to a value-delimited file."""
    data.to_csv(
        filename,
        index=False,
        sep=FIELD_SEPARATOR,
        quoting=QUOTE_NONE,
        na_rep="",
    )


def get_cache_filename(country_code):
    """Get the cache filename for a given country."""
    return CACHE_DIR / f"{country_code.lower()}.tsv"


def fetch_remote(country_code):
    """Fetch administrative units for a given country from OpenStreetMap using the Overpass API."""
    data = pd.DataFrame()
    for admin_level in ADMIN_LEVELS:
        query = f"""
        [out:csv(::id, ::type, "name", "name:en", "admin_level", "wikidata", "ref", "ref:nuts", "ISO3166-2")];
        area["ISO3166-1"="{country_code}"][boundary=administrative]->.country;
        (
        relation(area.country)[admin_level={admin_level}][boundary=administrative][type!=multilinestring];
        );
        out qt;
        """
        response = requests.post(OVERPASS_API_URL, data={"data": query})
        response.encoding = "utf-8"
        if response.status_code == 200:
            df = pd.read_csv(StringIO(response.text), sep=FIELD_SEPARATOR)
            if df.empty:
                continue
            df["ISO3166-1"] = country_code
            df["osm_id"] = df["@id"].apply(
                lambda x: f"{df['@type'].str[0].str.upper().values[0]}{x}"
            )
            data = pd.concat([data, df], ignore_index=True)
        else:
            logging.warning(f"Failed to fetch data for {country_code} - L{admin_level}")
    data = rename_columns(data)
    data = add_missing_columns(data)
    data = reorder_columns(data)
    if not data.empty:
        data = normalize_data(data)
        data = filter_rows(data)
        data = sort_rows(data)
    save_data(data, get_cache_filename(country_code))
    return data


def fetch_cached(country_code):
    """Fetch administrative units for a given country from the cache."""
    filename = get_cache_filename(country_code)
    if filename.exists():
        return pd.read_csv(filename, sep=FIELD_SEPARATOR, keep_default_na=False)
    else:
        return None


def main():
    if not INPUT_FILE.exists():
        logging.error(f"Input file not found: {INPUT_FILE}")
        return
    countries = pd.read_csv(INPUT_FILE, keep_default_na=False)
    data = pd.DataFrame()
    for country in countries.itertuples():
        country_code = country.alpha_2
        country_name = country.name
        logging.info(f"Fetching data for {country_name} ({country_code})")
        admin_units = fetch_cached(country_code)
        if admin_units is None:
            admin_units = fetch_remote(country_code)
            time.sleep(2)
        elif admin_units.empty:
            continue
        data = pd.concat([data, admin_units], ignore_index=True)
    data = sort_rows(data)
    save_data(data, OUTPUT_FILE)


if __name__ == "__main__":
    main()
