import csv
import json
import logging
import pandas as pd
import re
import requests
import sqlite3

from calendar import timegm
from pathlib import Path
from time import gmtime, sleep
from utils import get_coalesced


NOMINATIM_API_URL = "https://nominatim.openstreetmap.org"
DEFAULT_PARAMETERS = {
    "accept-language": "en",
    "format": "jsonv2",
    "polygon_geojson": 1,
    "addressdetails": 1,
    "namedetails": 0,
    "extratags": 0,
}
USER_AGENT = "MOOD Geocoder"
DEFAULT_ZOOM_LEVEL = 10
DEFAULT_RESULT_LIMIT = 1
REMOTE_REQUEST_DELAY = 2
OSM_ELEMENT_TYPES = {
    "node": "N",
    "way": "W",
    "relation": "R",
}


def fetch(url, params=None):
    """Fetch data from the web."""
    if params:
        params = DEFAULT_PARAMETERS | params
    else:
        params = DEFAULT_PARAMETERS
    timestamp = timegm(gmtime())
    headers = {"User-Agent": f"{USER_AGENT} #{timestamp}"}
    response = requests.get(url, params=params, headers=headers)
    logging.info(f"Requesting data from {response.url}")
    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {response.reason}")
    return response.json()


def lookup(osm_ids):
    """Look up a location using the Nominatim API."""
    url = f"{NOMINATIM_API_URL}/lookup"
    params = {"osm_ids": osm_ids}
    return fetch(url, params=params)


def search(query, country_codes=None, limit=DEFAULT_RESULT_LIMIT):
    """Search for a location using the Nominatim API."""
    url = f"{NOMINATIM_API_URL}/search"
    params = {"q": query, "limit": limit}
    if country_codes:
        params["countrycodes"] = country_codes
    return fetch(url, params=params)


def reverse_geocode(latitude, longitude, zoom=DEFAULT_ZOOM_LEVEL):
    """Reverse geocodes a location using the Nominatim API."""
    url = f"{NOMINATIM_API_URL}/reverse"
    params = {"lat": latitude, "lon": longitude, "zoom": zoom}
    return fetch(url, params=params)


def normalize_feature(feature):
    """Normalize a feature."""
    feature_id = create_feature_id(feature.get("osm_type"), feature.get("osm_id"))
    if not feature_id:
        return None
    return {
        "id": feature_id,
        "osm_id": feature.get("osm_id"),
        "osm_type": feature.get("osm_type"),
        "name": feature.get("display_name"),
        "address": json.dumps(feature.get("address")),
        "place_rank": feature.get("place_rank"),
        "latitude": feature.get("lat"),
        "longitude": feature.get("lon"),
        "bounding_box": json.dumps(feature.get("boundingbox")),
        "polygon": json.dumps(feature.get("geojson")),
    }


def create_feature_id(osm_type, osm_id):
    """Create a feature ID from an OSM element type and ID."""
    osm_type = OSM_ELEMENT_TYPES.get(osm_type)
    if osm_type is None:
        raise ValueError(f"Invalid OSM element type: {osm_type}")
    if type(osm_id) is not int:
        raise ValueError(f"Invalid OSM ID: {osm_id}")
    return f"{osm_type}{osm_id}"


def parse_feature_id(feature_id):
    """Parse a feature ID into an OSM element type and ID."""
    id_pattern = r"^[A-Z]\d+$"
    if not re.match(id_pattern, feature_id):
        raise ValueError(f"Invalid feature ID: {feature_id}")
    osm_type = feature_id[0]
    osm_id = feature_id[1:]
    if osm_type not in OSM_ELEMENT_TYPES.values():
        raise ValueError(f"Invalid OSM element type: {osm_type}")
    return (osm_type, osm_id)


def get_locality_name(address):
    """Get the locality name from an address."""
    return get_coalesced(address, ["city", "town", "village", "hamlet"])


def get_admin_level_1_name(address):
    """Get the administrative level 1 name from an address."""
    return get_coalesced(
        address,
        ["ISO3166-2-lvl4", "state", "region", "province", "ISO3166-2-lvl6", "county"],
    )


def get_country_name(address):
    """Get the country name from an address."""
    return address.get("country")


def initialize_database(connection, schema_file):
    """Initialize the cache database with the schema defined in the schema file."""
    cursor = connection.cursor()
    # Check if the database is empty
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    # Execute database schema creation script
    if not tables:
        with open(schema_file, "r") as file:
            cursor.executescript(file.read())
    cursor.close()
    connection.commit()


def enforce_foreign_keys(connection):
    """Enforce foreign key constraints on the cache database."""
    cursor = connection.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")
    cursor.close()
    connection.commit()


def get_record(cursor):
    """Return a dictionary representation of the current row in the cursor."""
    record = cursor.fetchone()
    if record:
        columns = [column[0] for column in cursor.description]
        record = dict(zip(columns, record))
    return record if record else None


def get_records(cursor):
    """Return a list of dictionary representations of the rows in the cursor."""
    records = cursor.fetchall()
    if records:
        columns = [column[0] for column in cursor.description]
        records = [dict(zip(columns, record)) for record in records]
    return records if records else None


def get_feature(
    connection, api_call, api_args, feature_id=None, term=None, term_type=None
):
    """Get a feature from the cache database or from the Nominatim API."""
    data_source = "cache"
    if feature_id:
        feature = get_feature_from_cache(connection, feature_id)
        term = feature_id
    else:
        feature = find_feature_in_cache(connection, term)
    if not feature:
        data_source = "remote source"
        sleep(REMOTE_REQUEST_DELAY)
        results = api_call(**api_args)
        if not results:
            logging.info(f'No results found for "{term}"')
            return dict()
        if type(results) == list:
            feature = results[0]
        else:
            feature = results
        feature = normalize_feature(feature)
        save_feature_to_cache(connection, feature, term, term_type)
    address = feature.get("address")
    if address:
        feature["address"] = json.loads(address)
    feature_label = f"{feature.get('id')} - {feature.get('name')}"
    logging.info(f'Fetched "{feature_label}" from {data_source}')
    return feature


def get_feature_from_cache(connection, feature_id):
    """Get a feature from the cache database."""
    cursor = connection.cursor()
    cursor.execute(
        "SELECT * FROM feature WHERE id = ?",
        (feature_id,),
    )
    feature = get_record(cursor)
    cursor.close()
    return feature


def get_features_from_cache(connection, feature_ids):
    """Get multiple features from the cache database."""
    cursor = connection.cursor()
    cursor.execute("CREATE TEMPORARY TABLE selected_feature (id TEXT)")
    cursor.executemany(
        "INSERT INTO selected_feature (id) VALUES (?)", [(id,) for id in feature_ids]
    )
    cursor.execute(
        """
            SELECT *
            FROM feature
                INNER JOIN selected_feature
                    ON feature.id = selected_feature.id
        """
    )
    features = get_records(cursor)
    cursor.execute("DROP TABLE selected_feature")
    cursor.close()
    return features


def find_feature_in_cache(connection, term):
    """Find a feature in the cache database."""
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT feature.*
        FROM feature_index
            INNER JOIN feature ON feature_index.feature_id = feature.id
        WHERE term = ?
        """,
        (term,),
    )
    feature = get_record(cursor)
    cursor.close()
    return feature


def save_feature_to_cache(connection, feature, term=None, term_type=None):
    """Save a feature to the cache database."""
    cursor = connection.cursor()
    statement = """
        INSERT OR IGNORE INTO feature (
            id,
            osm_id,
            osm_type,
            name,
            address,
            place_rank,
            latitude,
            longitude,
            bounding_box,
            polygon
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.execute(statement, tuple(feature.values()))
    if term and term_type:
        statement = """
            INSERT OR IGNORE INTO feature_index (
                term,
                term_type,
                feature_id
            ) VALUES (?, ?, ?)
        """
        cursor.execute(statement, (term, term_type, feature.get("id")))
    cursor.close()
    connection.commit()


def delete_feature_from_cache(connection, feature_id):
    """Delete a feature from the cache database."""
    cursor = connection.cursor()
    cursor.execute(
        "DELETE FROM feature WHERE id = ?",
        (feature_id,),
    )
    cursor.close()
    connection.commit()


def save_feature_geometries_to_disk(features, geometry_dir):
    for feature in features:
        with open(geometry_dir / f"{feature.get('id')}.json", "w") as file:
            content = {
                "bounding_box": json.loads(feature.get("bounding_box")),
                "polygon": json.loads(feature.get("polygon")),
            }
            json.dump(content, file, indent=2)


def normalize_empresi_data(df):
    """Normalize EMPRES-I data."""

    def compile_location(record):
        location = {
            "region": record["region"],
            "subregion": record["subregion"],
            "country": record["country"],
            "admin_level_1": record["admin_level_1"],
            "locality": record["locality"],
        }
        return json.dumps(location)

    df.rename(
        columns={
            "Event.ID": "original_record_id",
            "Disease": "disease",
            "Serotype": "serotype",
            "Region": "region",
            "Subregion": "subregion",
            "Country": "country",
            "Admin.level.1": "admin_level_1",
            "Locality": "locality",
            "Latitude": "latitude",
            "Longitude": "longitude",
            "Diagnosis.source": "diagnosis_source",
            "Diagnosis.status": "diagnosis_status",
            "Animal.type": "host_domestication_status",
            "Species": "species",
            "Observation.date..dd.mm.yyyy.": "observation_date",
            "Report.date..dd.mm.yyyy.": "report_date",
            "Humans.affected": "affected_human_count",
            "Human.deaths": "human_death_count",
        },
        inplace=True,
    )
    df["original_record_location_description"] = df.apply(compile_location, axis=1)
    df.drop(
        columns=[
            "disease",
            "region",
            "subregion",
            "country",
            "admin_level_1",
            "locality",
            "diagnosis_source",
            "diagnosis_status",
            "affected_human_count",
            "human_death_count",
        ],
        inplace=True,
    )
    return df


def geocode_empresi_data(df, connection):
    """Geocode EMPRES-I data."""
    locality_names = []
    locality_ids = []
    admin_level_1_names = []
    admin_level_1_ids = []
    country_names = []
    country_ids = []
    for index, row in df.iterrows():
        latitude = row["latitude"]
        longitude = row["longitude"]
        term = f"{latitude}, {longitude}"
        api_args = {"latitude": latitude, "longitude": longitude}
        locality = get_feature(
            connection, reverse_geocode, api_args, term=term, term_type="coordinate"
        )
        address = locality.get("address")
        locality_name = get_locality_name(address)
        admin_level_1_name = get_admin_level_1_name(address)
        country_name = address.get("country")
        query = f"{admin_level_1_name}, {country_name}"
        admin_level_1 = get_feature(
            connection, search, {"query": query}, term=query, term_type="query"
        )
        query = country_name
        country = get_feature(
            connection, search, {"query": query}, term=query, term_type="query"
        )
        locality_names.append(locality_name)
        admin_level_1_names.append(admin_level_1_name)
        country_names.append(country_name)
        locality_ids.append(locality.get("id"))
        admin_level_1_ids.append(admin_level_1.get("id"))
        country_ids.append(country.get("id"))
    df["locality"] = locality_names
    df["locality_osm_id"] = locality_ids
    df["admin_level_1"] = admin_level_1_names
    df["admin_level_1_osm_id"] = admin_level_1_ids
    df["country"] = country_names
    df["country_osm_id"] = country_ids
    return df


def reorder_columns(df):
    """Reorder columns in a DataFrame."""
    columns = [
        "observation_date",
        "report_date",
        "host_domestication_status",
        "latitude",
        "longitude",
        "country",
        "admin_level_1",
        "locality",
        "country_osm_id",
        "admin_level_1_osm_id",
        "locality_osm_id",
        "original_record_id",
        "original_record_location_description",
    ]
    return df[columns]

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    # Define directory paths
    package_dir = Path(__file__).parent
    root_dir = package_dir.parent
    script_dir = package_dir / "scripts"
    input_dir = root_dir / "data" / "input"
    output_dir = root_dir / "data" / "output"
    work_dir = root_dir / "data" / "work"
    auxiliary_dir = output_dir / "auxiliaries"
    geometry_dir = output_dir / "geometries"

    # Define file paths
    cache_db_schema_file = script_dir / "cache_db_schema.sql"
    cache_db_file = work_dir / "cache.db"
    input_file = input_dir / "empres-i" / "2022-06-07" / "avian-influenza--sample.csv"
    output_file = output_dir / "data.tsv"

    # Connect to the cache database
    connection = sqlite3.connect(cache_db_file)

    # Initialize the cache database
    initialize_database(connection, cache_db_schema_file)

    # Enable foreign key constraints
    enforce_foreign_keys(connection)

    # Load the input data
    df = pd.read_csv(input_file)
    df = df.head(10)

    # Normalize and geocode the input data
    df = normalize_empresi_data(df)
    df = geocode_empresi_data(df, connection)
    df = reorder_columns(df)

    # Save the output data
    df.to_csv(output_file, sep="\t", quoting=csv.QUOTE_NONE, index=False)

    # Save feature geometries to disk
    feature_ids = (
        pd.concat(
            [df["locality_osm_id"], df["admin_level_1_osm_id"], df["country_osm_id"]]
        )
        .dropna()
        .unique()
    )
    features = get_features_from_cache(connection, feature_ids)
    if features:
        save_feature_geometries_to_disk(features, geometry_dir)

    # Close the connection to the cache database
    connection.close()
