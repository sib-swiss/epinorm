import json
import logging
import re
import requests

from calendar import timegm
from time import gmtime, sleep
from epinorm.cache import SQLiteCache
from epinorm.utils import get_coalesced

NOMINATIM_API_URL = "https://nominatim.openstreetmap.org"
NOMINATIM_API_METHODS = ("lookup", "search", "reverse")
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
REMOTE_REQUEST_DELAY = 2 # in seconds
OSM_ELEMENT_TYPES = {
    "node": "N",
    "way": "W",
    "relation": "R",
}


class Geocoder:
    
    def fetch(self, url, params=None):
        """Fetch data from the web."""

        # sleep to make sure we respect the API requirements
        sleep(REMOTE_REQUEST_DELAY)

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


class NominatimGeocoder(Geocoder):

    def __init__(self):
        super().__init__()
        self._cache = SQLiteCache()

    def _get_api_method(self, method):
        """Get the Nominatim API method."""
        if method not in NOMINATIM_API_METHODS:
            raise ValueError(f"Invalid Nominatim API method: {method}")
        return getattr(self, method)

    def lookup(self, osm_ids):
        """Look up a location using the Nominatim API."""
        url = f"{NOMINATIM_API_URL}/lookup"
        params = {"osm_ids": osm_ids}
        return self.fetch(url, params=params)

    def search(self, query, country_codes=None, limit=DEFAULT_RESULT_LIMIT):
        """Search for a location using the Nominatim API."""
        url = f"{NOMINATIM_API_URL}/search"
        params = {"q": query, "limit": limit}
        if country_codes:
            params["countrycodes"] = country_codes
        return self.fetch(url, params=params)

    def reverse(self, latitude, longitude, zoom=DEFAULT_ZOOM_LEVEL):
        """Reverse geocode a location using the Nominatim API."""
        url = f"{NOMINATIM_API_URL}/reverse"
        params = {"lat": latitude, "lon": longitude, "zoom": zoom}
        return self.fetch(url, params=params)

    def get_detailed_address(self, feature_id):
        """
        Get the detailed address of an osm_id using the Nominatim API.
        The parameter feature_id of this method is of the form "R13310"
        """
        url = f"{NOMINATIM_API_URL}/details"
        params = {"osmtype": feature_id[0], "osmid": feature_id[1:], "addressdetails": 1, "format": "json"}
        result = self.fetch(url, params=params)
        return result["address"]

    def create_feature_id(self, osm_type, osm_id):
        """
        Create a feature ID from an OSM element type and ID.
        Sometimes osm_type is already correctly mapped ! The api responses are not consistent...
        """
        if osm_type not in OSM_ELEMENT_TYPES.values() and OSM_ELEMENT_TYPES.get(osm_type) is None:
            raise ValueError(f"Invalid OSM element type: {osm_type}")
        osm_type = OSM_ELEMENT_TYPES.get(osm_type, osm_type)

        if not isinstance(osm_id, int):
            raise ValueError(f"Invalid OSM ID: {osm_id}")

        return f"{osm_type}{osm_id}"

    def parse_feature_id(self, feature_id):
        """Parse a feature ID into an OSM element type and ID."""
        id_pattern = r"^[A-Z]\d+$"
        if not re.match(id_pattern, feature_id):
            raise ValueError(f"Invalid feature ID: {feature_id}")
        osm_type = feature_id[0]
        osm_id = feature_id[1:]
        if osm_type not in OSM_ELEMENT_TYPES.values():
            raise ValueError(f"Invalid OSM element type: {osm_type}")
        return (osm_type, osm_id)

    def normalize_feature(self, feature):
        """Normalize a feature."""

        feature_id = self.create_feature_id(
            feature.get("osm_type"), feature.get("osm_id")
        )
        if not feature_id:
            return None
        return {
            "id": feature_id,
            "osm_id": feature.get("osm_id"),
            "osm_type": feature.get("osm_type"),
            "name": feature.get("display_name"),
            "address": self.get_detailed_address(feature_id),
            "place_rank": feature.get("place_rank"),
            "latitude": feature.get("lat"),
            "longitude": feature.get("lon"),
            "bounding_box": json.dumps(feature.get("boundingbox")),
            "polygon": json.dumps(feature.get("geojson")),
        }

    def get_locality(self, detailed_address):
        """Get the locality name from a complex address."""

        # find all entries with a rank address
        entries_rank_address = []
        for entry in detailed_address:
            if "rank_address" in entry:
                entries_rank_address.append(entry) 
        if len(entries_rank_address) == 0:
            return (None, None)

        # we only want the entries with rank address from 13 to 25 (for locality)
        # https://nominatim.org/release-docs/latest/customize/Ranking/
        entries_in_range = list(filter(lambda entry : 13 <= entry["rank_address"] and \
                                                     entry["rank_address"] <= 25, entries_rank_address))
        if len(entries_in_range) == 0:
            return (None, None)

        # we prioritise the less precise one
        entry_locality = min(entries_in_range, key=lambda entry: entry["rank_address"])
        return (entry_locality["localname"], self.create_feature_id(entry_locality["osm_type"], entry_locality["osm_id"]))

    def get_admin_level_1(self, address_details, admin_level_sought=None):
        """Get the administrative level 1 name from a complex address."""

        if admin_level_sought is not None: # we are looking for a specific admin_level
            for entry in address_details:
                if entry.get("admin_level", -1) == admin_level_sought:
                    return (entry["localname"], self.create_feature_id(entry["osm_type"], entry["osm_id"]))
            return (None, None)

        else: # extract the highest admin level that exists in the country

            entires_admin_level = []
            for entry in address_details:
                if "admin_level" in entry:
                    entires_admin_level.append(entry) 
            if len(entires_admin_level) == 0:
                return (None, None)

            entry_min_admin_level = min(entires_admin_level, key=lambda entry: entry["admin_level"])
            return (entry_min_admin_level["localname"], self.create_feature_id(entry_min_admin_level["osm_type"], entry_min_admin_level["osm_id"]))

    def get_country_name(self, address):
        """
        Get the country name from an address. 
        It can only retrieve the country name, not the osm id.
        If it can't extract it, the method returns None
        """

        for entry in address:
            if entry["type"] == "country":
                return entry["localname"]
        return None

    def get_feature(
        self, api_method, api_args, feature_id=None, term=None, term_type=None
    ):
        """Get a feature from the cache database or from the Nominatim API."""

        data_source = "cache"
        if feature_id:
            feature = self._cache.get_feature(feature_id)
            term = feature_id
        else:
            feature = self._cache.find_feature(term)

        if not feature:
            data_source = "remote source"
            api_call = self._get_api_method(api_method)
            results = api_call(**api_args)
            if not results:
                logging.info(f'No results found for "{term}"')
                return dict()
            if isinstance(results, list):
                feature = results[0]
            else:
                feature = results
            # we don't want mountain ranges as results
            if feature.get("addresstype", "") == "mountain_range":
                logging.info(f'No results found for "{term}"')
                return dict()
            feature = self.normalize_feature(feature)
            self._cache.save_feature(feature, term, term_type)

        feature_label = f"{feature.get('id')} - {feature.get('name')}"
        logging.info(f'Fetched "{feature_label}" from {data_source}')
        return feature

    def get_cache(self):
        return self._cache
