import json
import pandas as pd
import numpy as np
import re
import itertools
import logging
from csv import QUOTE_NONE
from transliterate import translit
from transliterate.exceptions import LanguageDetectionError

from epinorm.geo import NominatimGeocoder
from epinorm.config import (
    REF_DATA_DIR, 
    COUNTRIES_FILE, 
    COUNTRIES_EXCEPTIONS, 
    ADMIN_LEVELS_FILE,
    ADMIN_LEVEL_1_FILE,
    NUTS_CODE_TO_COUNTRY_EXCEPTIONS,
    NUTS_COORDINATES_FILE,
    HOST_SPECIES_FILE,
    PATHOGEN_SPECIES_FILE
)

SAMPLING_MODES = ("top", "bottom", "random")
OUTPUT_FILE_SEPARATOR = "\t"
OUTPUT_COLUMNS = [
    "observation_date",
    "report_date",
    "pathogen_species_ncbi_id",
    "pathogen_species_name",
    "pathogen_serotype",
    "host_species_ncbi_id",
    "host_species_name",
    "host_species_common_name",
    "host_domestication_status",
    "latitude",
    "longitude",
    "country",
    "admin_level_1",
    "locality",
    "country_osm_id",
    "admin_level_1_osm_id",
    "locality_osm_id",
    "original_record_source",
    "original_record_id",
    "original_record_location_description",
]

def get_transliterated_endonym(row):
        """
        Transliterates the endonym of a row into latin characters.
        If it fails, it returns an empty list
        """

        # try to tranlisterate the endonym
        try:
            transliteration = translit(row["endonym"], reversed=True)

            # if the original language was russian, then it might have used the character ь which
            # doesn't get transliterate properly (it gets into an apostrophe)
            if "ь" in row["endonym"] and "'" in transliteration:
                transliteration = transliteration.replace("'", "")

            entry = {"name": transliteration, 
                    "admin_level":row["admin_level"], 
                    "osm_id": row["osm_id"]}
            return [entry]

        except (LanguageDetectionError, TypeError):
            return []

def get_admin_levels_table():
    """
    Retrieves the information in "countries.csv" and
    "administrative_units.tsv", and returns a dictionary that maps a
    country name (from "countries.csv") to a list of its administrative
    levels.
    Administrative levels are encoded as dictionaries with properties:
    {
        name,
        admin_level,
        osm_id
    }
    Also transliterates information from the administrative_units.tsv file.
    """

    # * create a mapping from country name to its two letter code
    country_to_code = pd.read_csv(COUNTRIES_FILE, index_col="name")["alpha_2"].to_dict()
    country_to_code.update(COUNTRIES_EXCEPTIONS) # add the exceptions

    # * now map country code to its admin levels
    code_to_admin = {}
    for country_code, rows_country in pd.read_table(ADMIN_LEVELS_FILE).groupby("iso3166_1_code"):

        code_to_admin[country_code] = []
        for _, row in rows_country.iterrows():

            new_entries = []

            if isinstance(row["exonym"], str): # make sure the column contains a value
                entry = {"name":row["exonym"],
                         "admin_level":row["admin_level"], 
                         "osm_id":row["osm_id"]}
                new_entries.append(entry)

            if isinstance(row["endonym"], str): # make sure the column contains a value
                entry = {"name": row["endonym"], 
                        "admin_level":row["admin_level"], 
                        "osm_id": row["osm_id"]}
                new_entries.append(entry)

                new_entries += get_transliterated_endonym(row)

            # some words are okay to be replaced, they are modifiers
            replacements = {
                "Oblast" : "Region",
                "Krai" : "Region",
            }
            new_entries_synonyms = []
            for entry in new_entries:
                for original, synonym in replacements.items():
                    pattern = re.compile("\\b" + original + "\\b")
                    if re.match(pattern, entry["name"]):
                        new_entry = {"name": re.sub(pattern, synonym, entry["name"]), 
                                     "admin_level":entry["admin_level"], 
                                     "osm_id":entry["osm_id"]}
                        new_entries_synonyms.append(new_entry)

            code_to_admin[country_code] += new_entries + new_entries_synonyms

    # * now map each country name to its admin levels by merging the datasets above
    country_to_admin = {}
    for country_name, country_code in country_to_code.items():
        country_to_admin[country_name] = code_to_admin.get(country_code, [])

    return country_to_admin

def get_nuts_to_coordinates():
    """
    This method reads the content of the NUTS_LB_2021_4326 file and returns a dictionary of
    { nuts_code: { longiture: int, latitude: int } }
    """

    nuts_to_coordinate = {}

    with open(NUTS_COORDINATES_FILE, "r") as f:
        data = json.load(f)

        for entry in data["features"]:

            nuts_code = entry["properties"]["NUTS_ID"]
            longitude = entry["geometry"]["coordinates"][0]
            latitude = entry["geometry"]["coordinates"][1]

            nuts_to_coordinate[nuts_code] = {"longitude": longitude, "latitude": latitude}

    return nuts_to_coordinate

def get_nuts_to_admin_level_1():
    """
    This returns a mapping from nuts code to its admin_level as written in the 
    'administrative_units' file.
    { nuts_code : { name: str, id: str } }
    """

    admin_units = pd.read_table(ADMIN_LEVELS_FILE)
    admin_units = admin_units.replace(np.nan, None) # its easier to deal with None

    nuts_to_admin_level_1 = {}

    for _, row in admin_units.iterrows():
        if row["nuts_code"] is not None:
            for nuts_code in row["nuts_code"].split(";"):
                nuts_to_admin_level_1[nuts_code] = {
                    "name": row["exonym"] if row["exonym"] else row["endonym"],
                    "id" : row["osm_id"]
                }

    return nuts_to_admin_level_1



class DataHandler:

    INPUT_COLUMNS = {}

    def __init__(self, data_file):
        self._geocoder = NominatimGeocoder()
        self._data = pd.read_csv(data_file)
        self._host_species = pd.read_csv(HOST_SPECIES_FILE).drop_duplicates(
            subset="host_species_synonym", keep="first"
        )
        self._pathogen_species = pd.read_csv(PATHOGEN_SPECIES_FILE).drop_duplicates(
            subset="pathogen_species_synonym", keep="first"
        )

    def _join_reference_data(self):
        self._data = pd.merge(
            self._data,
            self._host_species,
            how="left",
            on="host_species_synonym",
        )
        self._data = pd.merge(
            self._data,
            self._pathogen_species,
            how="left",
            on="pathogen_species_synonym",
        )

    def get_data(self):
        return self._data

    def set_data(self, data):
        self._data = data

    def save_data(self, output_file):
        self._data.to_csv(
            output_file, sep=OUTPUT_FILE_SEPARATOR, quoting=QUOTE_NONE, index=False
        )

    def save_geometries(self, output_dir):
        cache = self._geocoder.get_cache()
        feature_ids = self.get_feature_ids()
        features = cache.get_features(feature_ids)
        output_dir.mkdir(parents=True, exist_ok=True)
        for feature in features:
            with open(output_dir / f"{feature.get('id')}.json", "w") as file:
                content = {
                    "bounding_box": json.loads(feature.get("bounding_box")),
                    "polygon": json.loads(feature.get("polygon")),
                }
                json.dump(content, file, indent=2)

    def get_column_labels(self):
        return self._data.columns.tolist()

    def rename_columns(self):
        self._data.rename(columns=self.INPUT_COLUMNS, inplace=True)

    def delete_columns(self, columns):
        self._data.drop(columns=columns, inplace=True)

    def filter_columns(self, columns=OUTPUT_COLUMNS):
        self._data = self._data[columns]

    def sample_rows(self, mode, n):
        if mode not in SAMPLING_MODES:
            raise ValueError(f"Invalid sampling mode: {mode}")
        if mode == "top":
            return self._data.head(n)
        if mode == "bottom":
            return self._data.tail(n)
        if mode == "random":
            return self._data.sample(n)

    def get_feature_ids(self):
        return (
            pd.concat(
                [
                    self._data["locality_osm_id"],
                    self._data["admin_level_1_osm_id"],
                    self._data["country_osm_id"],
                ]
            )
            .dropna()
            .unique()
        )


class EmpresiDataHandler(DataHandler):

    INPUT_COLUMNS = {
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
    }

    def __init__(self, data_file):
        super().__init__(data_file)

    def _compile_location(self, record):
        location = {
            "region": record["region"],
            "subregion": record["subregion"],
            "country": record["country"],
            "admin_level_1": record["admin_level_1"],
            "locality": record["locality"],
        }
        return json.dumps(location)

    def _compile_serotype(self, serotype):
        if pd.isna(serotype):
            return None
        serotype = re.sub(r"\s+", " ", serotype).strip()
        if not serotype:
            return None
        match = re.match(r"^(H\d+)(N\d+) (\w+)$", serotype)
        if not match:
            return None
        return json.dumps(
            {
                "h_subtype": match.group(1),
                "n_subtype": match.group(2),
                "pathogenicity": match.group(3),
            }
        )

    def _normalize_dates(self):
        self._data["observation_date"] = pd.to_datetime(
            self._data["observation_date"], format="%d/%m/%Y", errors="coerce"
        ).dt.date
        self._data["report_date"] = pd.to_datetime(
            self._data["report_date"], format="%d/%m/%Y", errors="coerce"
        ).dt.date

    def _normalize_species(self):
        self._data["host_species_synonym"] = self._data["species"].str.lower()
        self._data["pathogen_species_synonym"] = (
            self._data["serotype"].str.split().str[0]
        )
        self._data["pathogen_serotype"] = self._data["serotype"].apply(
            self._compile_serotype
        )

    def _add_source_details(self):
        self._data["original_record_source"] = "EMPRES-i"
        self._data["original_record_location_description"] = self._data.apply(
            self._compile_location, axis=1
        )

    def _geocode(self):
        """Geocode EMPRES-i data."""

        country_name_to_code = pd.read_csv(COUNTRIES_FILE, index_col="name")["alpha_2"].to_dict()
        country_name_to_code.update(COUNTRIES_EXCEPTIONS) # add the exceptions

        admin_level_1_table = pd.read_csv(ADMIN_LEVEL_1_FILE)
        admin_level_1_table = admin_level_1_table.replace(np.nan, None) # so its easier to deal with those values

        # output columns
        locality_names = np.full(len(self._data), None)
        locality_ids = np.full(len(self._data), None)
        admin_level_1_names = np.full(len(self._data), None)
        admin_level_1_ids = np.full(len(self._data), None)
        country_names = np.full(len(self._data), None)
        country_ids = np.full(len(self._data), None)

        for i, row in self._data.iterrows():

            latitude = row["latitude"]
            longitude = row["longitude"]
            if pd.isna(latitude) or pd.isna(longitude):
                logging.warning(f"Geocoding error: sample {i} didn't have longitude and latitude info")
                continue

            # reverse search using latitude and longitude
            term = f"{latitude}, {longitude}"
            api_args = {"latitude": latitude, "longitude": longitude}
            locality = self._geocoder.get_feature(
                "reverse", api_args, term=term, term_type="coordinate"
            )
            if not locality:
                logging.warning(f"Geocoding error: API coudln't retrieve location of sample {i} using coordinates")
                continue
            address = locality.get("address")

            # get the country osm_id
            country_name = self._geocoder.get_country_name(address)
            if country_name is None:
                logging.warning(f"Geocoding error: API coudln't retrieve country of address for sample {i}")
                continue
            api_args = {"query": country_name}
            location_country = self._geocoder.get_feature(
                "search", api_args, term=country_name, term_type="query"
            )
            if not location_country:
                logging.warning(f"Geocoding error: couldn't use api to get country osm_id for sample {i}")
                continue
            country_names[i] = country_name
            country_ids[i] = location_country.get("id")

            # get locality
            (locality_name, locality_id) = self._geocoder.get_locality(address)
            locality_names[i] = locality_name # this could be None
            locality_ids[i] = locality_id # this could be None

            # admin level 1
            if country_name not in country_name_to_code:
                logging.warning(f"Geocoding error: don't know country code of country {country_name} in sample {i}")
                continue
            country_code = country_name_to_code[country_name]
            admin_level_1_sought = admin_level_1_table[admin_level_1_table["country_code"] == country_code]["osm_level"].iloc[0]
            (admin_level_1_name, admin_level_1_id) = self._geocoder.get_admin_level_1(address, admin_level_1_sought)
            admin_level_1_names[i] = admin_level_1_name # this could be None
            admin_level_1_ids[i] = admin_level_1_id # this could be None

        self._data["locality"] = locality_names
        self._data["locality_osm_id"] = locality_ids
        self._data["admin_level_1"] = admin_level_1_names
        self._data["admin_level_1_osm_id"] = admin_level_1_ids
        self._data["country"] = country_names
        self._data["country_osm_id"] = country_ids

    def normalize(self):
        self.rename_columns()
        self._normalize_dates()
        self._normalize_species()
        self._join_reference_data()
        self._add_source_details()
        self._geocode()
        self.filter_columns()


class GenBankDataHandler(DataHandler):

    INPUT_COLUMNS = {
        "Pathogen NCBI taxonomy ID": "pathogen_species_ncbi_id",
        "Pathogen species": "pathogen_species_name",
        "Pathogen serotype": "pathogen_serotype",
        "Pathogen isolate or strain": "pathogen_strain",
        "Host species Latin name": "host_species_name",
        "Host species NCBI taxonomy ID": "host_species_ncbi_id",
        "Date observed": "observation_date",
        "Geo text original": "original_location",
    }

    def __init__(self, data_file):
        super().__init__(data_file)

    def _compile_location(self, record):
        """
        extract the information from the original_location field (according to the genbank format)
        the format is:
        country[:region [, location]]

        we will extract them into a json object with fields:
        {
            country
            areas
        }
        """

        location = {"country": None, "areas":[]}

        if ":" in record["original_location"]:
            country, areas = record["original_location"].split(":", 1)
            location["country"] = country
            location["areas"] = areas.split(",") # even if there isn't a comma you get what we need

        else:
            location["country"] = record["original_location"]

        return json.dumps(location)

    def _get_location_from_strain(self, record):
        strain = str(record["pathogen_strain"])
        tokens = re.sub(r"[^A-Za-z]+", "-", strain).split("-")
        for token in tokens:
            if re.match(r"^[A-Z][a-z]{4,}$", token):
                return token

    def _format_location(self, record):
        """
        basically we combine the information from original_record_location_description and the extracted information
        of the strain. We also clean them (removing unwanted characters)

        we return a dictionaly in json format with all the fields
        {
            country
            areas
        }
        """

        def clean_token(token):
            token = token.replace("_", " ")
            token = re.sub(r"\(.+\)", "", token) 
            token = re.sub(r"[^\w\s-]+", "", token)
            token = token.strip()
            token = token.lower() # for easier comparison later
            return token

        location = json.loads(record["original_record_location_description"])

        # format country to make comparison easier
        location["country"] = clean_token(location["country"])

        # format 'areas' and 'extracted field'
        cleaned_places = [] 
        places_to_clean = location["areas"]
        if record["extracted_location"]:
            places_to_clean.append(record["extracted_location"])

        for place in places_to_clean:

            cleaned_place = clean_token(place)
            
            if cleaned_place != "" and cleaned_place != location["country"] and \
               cleaned_place not in cleaned_places:
                cleaned_places.append(cleaned_place)

        # take edge cases into account
        exception_mappings = {
            "west siberia": "siberian federal district",
            "east siberia": "siberian federal district",
        }
        cleaned_places = [ exception_mappings.get(token, token) for token in cleaned_places ]

        location["areas"] = cleaned_places

        return json.dumps(location)

    def _normalize_dates(self):
        self._data["observation_date"] = pd.to_datetime(
            self._data["observation_date"], errors="coerce"
        ).dt.date
        self._data["report_date"] = pd.NaT

    def _add_source_details(self):
        self._data["original_record_source"] = "GenBank"
        self._data["original_record_location_description"] = self._data.apply(
            self._compile_location, axis=1
        )

    def _add_missing_columns(self):
        self._data["host_species_common_name"] = None
        self._data["host_domestication_status"] = None
        self._data["latitude"] = None
        self._data["longitude"] = None
        self._data["original_record_id"] = None

    def _search_tokens_diff_order(self, areas, fixed_text):
        """
        This method uses the geocoder to make a query of areas (a list of tokens) and fixed_text (a string).
        we vary the order of the tokens in areas as they could be in the wrong order. We also remove them
        if we still don't have a result. The code tries all permutations up to the point where it doesn't
        include tokens from areas anymore (only "fixed_text").
        """

        for length in range(len(areas), -1, -1):
            for permutation in itertools.permutations(areas, length):

                query = ", ".join(permutation) + ", " + fixed_text
                locality = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                if locality:
                    return locality
                
        # you are here if none of the permutations gave a result on Nominatim
        return None

    def _find_location_info(self, areas, fixed_text, admin_level_sought):
        """
        This method is called when we couldn't match any token from the adminstrative file.
        Here we have to find the full locality only from "unindentified" tokens.
        """

        result = {
            "locality": None,
            "locality_osm_id": None,
            "admin_level_1": None,
            "admin_level_1_osm_id": None,
        }

        # we must use an unstructured search as we have no idea what the tokens could be
        # we first reverse its order as in the dataset the most precise is last (compared to nominatim)
        locality = self._search_tokens_diff_order(areas[::-1], fixed_text)
        if not locality:
            return result
        address = locality.get("address")

        # extract locality (maybe there is none)
        (locality_name, locality_id) = self._geocoder.get_locality(address)
        if locality_name is not None:
            result["locality"] = locality_name
            result["locality_osm_id"] = locality_id
        
        # extract admin_level_1 (maybe there is none)
        (admin_level_1_name,  admin_level_1_id) = self._geocoder.get_admin_level_1(address, admin_level_sought)
        if admin_level_1_name is not None:
            result["admin_level_1"] = admin_level_1_name
            result["admin_level_1_osm_id"] = admin_level_1_id

        return result

    def _geocode(self):
        """Geocode GenBank data."""

        # extract information from the strain (could be whatever)
        self._data["extracted_location"] = self._data.apply(
            self._get_location_from_strain, axis=1
        )
        # compile all the information we know about the location inside this column
        self._data["location"] = self._data.apply(self._format_location, axis=1)

        admin_levels_table = get_admin_levels_table()

        admin_level_1_table = pd.read_csv(ADMIN_LEVEL_1_FILE)
        admin_level_1_table = admin_level_1_table.replace(np.nan, None) # so its easier to deal with those values

        country_to_code = pd.read_csv(COUNTRIES_FILE, index_col="name")["alpha_2"].to_dict()
        country_to_code.update(COUNTRIES_EXCEPTIONS) # add the exceptions

        # initialise to zero all output columns
        country_names = np.full(len(self._data), None) 
        country_ids = np.full(len(self._data), None) 
        admin_level_1s = np.full(len(self._data), None) 
        admin_level_1_ids = np.full(len(self._data), None) 
        localities = np.full(len(self._data), None) 
        locality_osm_ids = np.full(len(self._data), None) 

        for i, row in self._data.iterrows():
            
            location = json.loads(row["location"])
            if location["country"] is None: # we now it doesn't contain any info then
                logging.warning(f"Geocoding error: sample {i} didn't have country information")
                continue

            # get the country osm_id
            api_args = {"query": location["country"]}
            location_country = self._geocoder.get_feature(
                "search", api_args, term=location["country"], term_type="query"
            )
            if not location_country:
                logging.warning(f"Geocoding error: api coudln't get country osm id of sample {i}")
                continue
            country_names[i] = country_name = location_country.get("name")
            country_ids[i] = location_country.get("id")

            areas = location["areas"]
            if len(areas) == 0: # this is not considered a geocoding error
                continue

            # get the admin_level_1 for that country
            if country_name not in country_to_code:
                logging.warning(f"Geocoding error: don't know country code of country in sample {i}")
                continue
            country_code = country_to_code[country_name]
            admin_level_1_sought = admin_level_1_table[admin_level_1_table["country_code"] == country_code]["osm_level"].iloc[0]

            # try to match tokens in 'areas' with the administrative_units file
            admin_levels_found = []
            if country_name in admin_levels_table:
                for admin_level in admin_levels_table[country_name]:
                    if admin_level["name"].lower() in areas:
                        areas.remove(admin_level["name"].lower()) # this removes one occurance on purpose
                        admin_levels_found.append(admin_level)

            # we couldn't identify any token, we must use the api using all oringal fields
            if len(admin_levels_found) == 0:
                result = self._find_location_info(areas, country_name, admin_level_1_sought)
                localities[i] = result["locality"]
                locality_osm_ids[i] = result["locality_osm_id"]
                admin_level_1s[i] = result["admin_level_1"]
                admin_level_1_ids[i] = result["admin_level_1_osm_id"]

                # record if oddities happened
                if all([ value is None for value in result.values()]):
                    logging.warning(f"Geocoding error: couldn't extract any locality or admin_level_1 information for sample {i}")
                elif result["locality"] is not None and result["admin_level_1"] is None:
                    logging.warning(f"Geocoding error: could extract locality but not admin_level_1 information for sample {i}")

                continue
            
            # now we check if we found the admin_level_1 of the country
            entry_admin_level_sought = None
            for entry in admin_levels_found:
                if entry["admin_level"] == admin_level_1_sought:
                    entry_admin_level_sought = entry

            if entry_admin_level_sought is not None: 
                admin_level_1s[i] = entry_admin_level_sought["name"]
                admin_level_1_ids[i] = entry_admin_level_sought["osm_id"]

                # now check if you also have a locality to examine (it must remain in 'areas')
                # there could be other entires in admin_level_found but they can't be a locality
                if len(areas) == 0: 
                    continue
                
                # do a search using nominatim to find locality
                sorted(admin_levels_found, key=lambda x: x["admin_level"], reverse=True)
                admin_levels_found_names = list(map(lambda x: x["name"], admin_levels_found))
                fixed_location_text = ", ".join(admin_levels_found_names) +  ", " + country_name
                locality = self._search_tokens_diff_order(areas[::-1], fixed_location_text)
                if not locality:
                    logging.warning(f"Geocoding error: couldn't identify tokens after admin_level_1 in sample {i}")
                    continue
                address = locality.get("address")

                # extract locality (maybe there is none, it means extra tokens were lower than admin_1 but higher than locality)
                (locality_name, locality_id) = self._geocoder.get_locality(address)
                if locality_name is not None:
                    localities[i] = locality_name
                    locality_osm_ids[i] = locality_id
                
                continue

            # we are here if we haven't identified the admin_level_1 yet
            # we then do a normal search and extract it from the api

            sorted(admin_levels_found, key=lambda x: x["admin_level"], reverse=True)
            admin_levels_found_names = list(map(lambda x: x["name"], admin_levels_found))
            fixed_location_text = ", ".join(admin_levels_found_names) +  ", " + country_name
            result = self._find_location_info(areas, fixed_location_text, admin_level_1_sought)
            localities[i] = result["locality"]
            locality_osm_ids[i] = result["locality_osm_id"]
            admin_level_1s[i] = result["admin_level_1"]
            admin_level_1_ids[i] = result["admin_level_1_osm_id"]

            # record if oddities happened
            if all([ value is None for value in result.values()]):
                logging.warning(f"Geocoding error: couldn't extract any locality or admin_level_1 information for sample {i}")
            elif result["locality"] is not None and result["admin_level_1"] is None:
                logging.warning(f"Geocoding error: could extract locality but not admin_level_1 information for sample {i}")


        self._data["country"] = country_names
        self._data["country_osm_id"] = country_ids
        self._data["admin_level_1"] = admin_level_1s
        self._data["admin_level_1_osm_id"] = admin_level_1_ids
        self._data["locality"] = localities
        self._data["locality_osm_id"] = locality_osm_ids

    def normalize(self):
        self.rename_columns()
        self._normalize_dates()
        self._add_source_details()
        self._add_missing_columns()
        self._geocode()
        self.filter_columns()


class ECDCDataHandler(DataHandler):
    INPUT_COLUMNS = {
        "Subject": "pathogen_species_synonym",
        "Classification": "classification",
        "DateOfDiagnosisISOdate": "date_of_diagnosis",
        "DateOfNotificationISOdate": "date_of_notification",
        "DateOfOnsetISOdate": "date_of_onset",
        "Imported": "imported",
        "PlaceOfInfection": "place_of_infection",
        "PlaceOfInfectionEVD": "place_of_infection_evd",
        "PlaceOfNotification": "place_of_notification",
        "ReportingCountry": "reporting_country",
        # Additions
        "EventID": "original_record_id",
        "Species": "host_species_synonym",
        "DomesticationStatus": "host_domestication_status",
        "Latitude": "latitude",
        "Longitude": "longitude",
    }

    def __init__(self, data_file):
        super().__init__(data_file)

    def _resolve_location(self, record):
        """
        choose the nuts code from the correct column, and the one that is correctly formatted
        """

        location = record["place_of_infection"]
        if not location:
            location = record["place_of_infection_evd"]
        if not location and record["imported"] == "N":
            location = record["place_of_notification"]
        if not location and record["imported"] == "N":
            location = record["reporting_country"]

        if not pd.isna(location) and not re.match(r"([A-Z]{2})", location):
            return None

        return location

    def _compile_location(self, record):
        location = {
            "place_of_infection": record["place_of_infection"],
            "place_of_infection_evd": record["place_of_infection_evd"],
            "place_of_notification": record["place_of_notification"],
            "reporting_country": record["reporting_country"],
        }
        for key, value in list(location.items()):
            if value is None:
                del location[key]
        return json.dumps(location)

    def _normalize_location(self):
        missing_values = [pd.NA, "NULL", "UNK", "UNK_DJ"]
        columns = [
            "place_of_infection",
            "place_of_infection_evd",
            "place_of_notification",
        ]
        for column in columns:
            self._data[column] = self._data[column].replace(missing_values, None)
        self._data["location"] = self._data.apply(self._resolve_location, axis=1)

    def _resolve_observation_date(self, record):
        dates = []
        if record["date_of_diagnosis"]:
            dates.append(record["date_of_diagnosis"])
        if record["date_of_onset"]:
            dates.append(record["date_of_onset"])
        dates = [date for date in dates if not pd.isna(date)]
        if not dates:
            return record["date_of_notification"]
        return min(dates)

    def _normalize_dates(self):
        self._data["date_of_diagnosis"] = pd.to_datetime(
            self._data["date_of_diagnosis"], errors="coerce"
        ).dt.date
        self._data["date_of_onset"] = pd.to_datetime(
            self._data["date_of_onset"], errors="coerce"
        ).dt.date
        self._data["date_of_notification"] = pd.to_datetime(
            self._data["date_of_notification"], errors="coerce"
        ).dt.date
        self._data["observation_date"] = self._data.apply(
            self._resolve_observation_date, axis=1
        )
        self._data["report_date"] = self._data["date_of_notification"]

    def _normalize_species(self):
        self._data["host_species_synonym"] = "human"
        self._data["pathogen_serotype"] = None

    def _add_source_details(self):
        self._data["original_record_source"] = "ECDC"
        self._data["original_record_location_description"] = self._data.apply(
            self._compile_location, axis=1
        )

    def _add_missing_columns(self):
        missing_columns = set(self.INPUT_COLUMNS.keys()) - set(self._data.columns)
        for column in missing_columns:
            self._data[column] = None

    def _geocode(self):
        """Geocode ECDC data."""

        admin_level_1_table = pd.read_csv(ADMIN_LEVEL_1_FILE)
        admin_level_1_table = admin_level_1_table.replace(np.nan, None) # so its easier to deal with those values

        nuts_to_coordinates = get_nuts_to_coordinates()

        country_name_to_code = pd.read_csv(COUNTRIES_FILE, index_col="name")["alpha_2"].to_dict()
        country_name_to_code.update(COUNTRIES_EXCEPTIONS) 
        country_code_to_name = { code: name for name, code in country_name_to_code.items() } # inverse mapping

        nuts_to_admin_level_1 = get_nuts_to_admin_level_1()

        # these are the columns we will fill in
        country_names = np.full(len(self._data), None) 
        country_ids = np.full(len(self._data), None)
        admin_level_1_names = np.full(len(self._data), None) 
        admin_level_1_ids = np.full(len(self._data), None)

        for i, row in self._data.iterrows():

            nuts_code = row["location"]
            if nuts_code is None:
                continue

            # first find in which country we are. We can't use the first two characters of the nuts code
            # as for instance Guadeloupe uses FRY1 (which is the country code of France).
            # Hence we use the coordinates of that nuts_code to find the country

            if nuts_code not in nuts_to_coordinates: 

                # we are here for multiple reasons:
                #                   - the NUTS code is invalid (we can't do anthing)
                #                   - the NUTS code is not for the year 2021 (we currently only support 2021 in our code)          
                #                   - the NUTS code only contains the country code

                if len(nuts_code) == 2: # assume we received only the country code (in NUTS)
                    country_code = nuts_code
                    if nuts_code in NUTS_CODE_TO_COUNTRY_EXCEPTIONS:
                        country_code = NUTS_CODE_TO_COUNTRY_EXCEPTIONS[nuts_code]
                    if country_code not in country_code_to_name:
                        logging.warning(f"Geocoding error: couldn't get country name from country code of sample {i}")
                        continue
                    country_name = country_code_to_name[country_code]

                    # get country osm id of the country from the api
                    api_args = {"query": country_name}
                    location_country = self._geocoder.get_feature(
                        "search", api_args, term=country_name, term_type="query"
                    )
                    if not location_country:
                        logging.warning(f"Geocoding error: couldn't get country osm id from its name for sample {i}")
                        continue
                    country_names[i] = location_country.get("name")
                    country_ids[i] = location_country.get("id")
                    continue

                # you are here for the two other reasons above
                logging.warning(f"Geocoding error: couldn't get coordinates of nuts code in sample {i}")
                continue

            coordinates = nuts_to_coordinates[nuts_code]
            term = f"{coordinates["latitude"]}, {coordinates["longitude"]}"
            locality_NUTS = self._geocoder.get_feature(
                "reverse", coordinates, term=term, term_type="coordinate"
            )
            if not locality_NUTS:
                logging.warning(f"Geocoding error: couldn't reverse search from coordinates from sample {i}")
                continue
            country_names[i] = country_name = self._geocoder.get_country_name(locality_NUTS["address"])
            if country_name is None:
                logging.warning(f"Geocoding error: couldn't extract country_name from address in sample {i}")
                continue
            country_code = country_name_to_code[country_name]

            # get osm id of that country name
            query = country_name
            locality_country = self._geocoder.get_feature(
                "search", {"query": query}, term=query, term_type="query"
            )
            if not locality_country:
                logging.warning(f"Geocoding error: couldn't get country osm id from country_name for sample {i}")
                continue
            country_ids[i] = locality_country["id"]

            # use the table to see what osm level you must take for the address
            admin_level_1_country = admin_level_1_table[admin_level_1_table["country_code"] == country_code]
            nuts_level_sought = admin_level_1_country["nuts_level"].iloc[0] # this could be None
            osm_level_sought = admin_level_1_country["osm_level"].iloc[0] # this could be None

            if nuts_level_sought is not None:
                if len(nuts_code) < nuts_level_sought + 2: # nuts code provided not precise enough
                    logging.warning(f"Geocoding error: nuts_code not precise enougn, dropping sample {i}")
                    continue
                elif len(nuts_code) > nuts_level_sought + 2: # nuts code provided is too precise
                    nuts_code = nuts_code[:int(nuts_level_sought) + 2]
            else:
                logging.warning(f"Geocoding error: couldn't determine nuts code for country code {country_code} in sample {i}")

            if nuts_code in nuts_to_admin_level_1: # we actually already know the mapping
                admin_level_1_names[i] = nuts_to_admin_level_1[nuts_code]["name"]
                admin_level_1_ids[i] = nuts_to_admin_level_1[nuts_code]["id"]

            else: # we extract the mapping from the address of the coordinates
                admin_level_1_name, admin_level_1_id = \
                            self._geocoder.get_admin_level_1(locality_NUTS["address"], osm_level_sought)
                if admin_level_1_name is None:
                    logging.warning(f"Geocoding error: couldn't extract amdin_level_sought from address in sample {i}")
                admin_level_1_names[i] = admin_level_1_name
                admin_level_1_ids[i] = admin_level_1_id

        self._data["country"] = country_names
        self._data["country_osm_id"] = country_ids
        self._data["admin_level_1"] = admin_level_1_names
        self._data["admin_level_1_osm_id"] = admin_level_1_ids
        self._data["locality"] = None
        self._data["locality_osm_id"] = None

    def _filter_rows(self):
        self._data = self._data[
            (self._data["classification"] == "CONF")
            & ~self._data["observation_date"].isna()
            & ~self._data["country"].isna()
        ]

    def normalize(self):
        self._add_missing_columns()
        self.rename_columns()
        self._normalize_dates()
        self._normalize_location()
        self._normalize_species()
        self._join_reference_data()
        self._add_source_details()
        self._geocode()
        self._filter_rows()
        self.filter_columns()
