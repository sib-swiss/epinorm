import json
import pandas as pd
import re
import itertools
from csv import QUOTE_NONE
from transliterate import translit
from transliterate.exceptions import LanguageDetectionError

from epinorm.geo import NominatimGeocoder
from epinorm.config import MIN_ADMIN_EXCEPTIONS, REF_DATA_DIR, COUNTRIES_DATA, COUNTRIES_EXCEPTIONS, ADMIN_LEVELS_DATA


HOST_SPECIES_FILE = REF_DATA_DIR / "ncbi_host_species.csv"
PATHOGEN_SPECIES_FILE = REF_DATA_DIR / "ncbi_pathogen_species.csv"

SAMPLING_MODES = ("top", "bottom", "random")
OUTPUT_FILE_SEPARATOR = "\t"
OUTPUT_COLUMNS = [
    # "observation_date",
    # "report_date",

    # "pathogen_species_ncbi_id",
    # "pathogen_species_name",
    # "pathogen_serotype",

    # "host_species_ncbi_id",
    # "host_species_name",
    # "host_species_common_name",
    # "host_domestication_status",

    # "latitude",
    # "longitude",
    "country",
    "country_osm_id",
    "locality",
    "locality_osm_id",
    "admin_level_1",
    "admin_level_1_osm_id",

    # "original_record_source",
    # "original_record_id",
    "original_record_location_description",
]

def get_translitaration_endonym(row):
        """
        this tries to transliterate the endonym of a row into latin characters.
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
    country_to_code = pd.read_csv(COUNTRIES_DATA, index_col="name")["alpha_2"].to_dict()
    country_to_code.update(COUNTRIES_EXCEPTIONS) # add the exceptions

    # * now map country code to its admin levels
    code_to_admin = {}
    for country_code, rows_country in pd.read_table(ADMIN_LEVELS_DATA).groupby("iso3166_1_code"):

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

                new_entries += get_translitaration_endonym(row)

            # some words are okay to be replaced, they are modifiers
            replacements = {
            "District" : "Region",
            "Region" : "District",
            }
            new_entries_synonyms = []
            for entry in new_entries:
                for original, synonym in replacements.items():
                    if original in entry["name"]:
                        new_entry = {"name":entry["name"].replace(original, synonym),
                                     "admin_level":entry["admin_level"], 
                                     "osm_id":entry["osm_id"]}
                        new_entries_synonyms.append(new_entry)

            code_to_admin[country_code] += new_entries + new_entries_synonyms

    # * now map each country name to its admin levels by merging the datasets above
    country_to_admin = {}
    for country_name, country_code in country_to_code.items():
        country_to_admin[country_name] = code_to_admin.get(country_code, [])

    return country_to_admin




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
        locality_names = []
        locality_ids = []
        admin_level_1_names = []
        admin_level_1_ids = []
        country_names = []
        country_ids = []
        for index, row in self._data.iterrows():
            latitude = row["latitude"]
            longitude = row["longitude"]
            term = f"{latitude}, {longitude}"
            api_args = {"latitude": latitude, "longitude": longitude}
            locality = self._geocoder.get_feature(
                "reverse", api_args, term=term, term_type="coordinate"
            )
            address = locality.get("address")
            locality_name = self._geocoder.get_locality_name(address)
            admin_level_1_name = self._geocoder.get_admin_level_1_name(address)
            country_name = address.get("country")
            query = f"{admin_level_1_name}, {country_name}"
            admin_level_1 = self._geocoder.get_feature(
                "search", {"query": query}, term=query, term_type="query"
            )
            query = country_name
            country = self._geocoder.get_feature(
                "search", {"query": query}, term=query, term_type="query"
            )
            locality_names.append(locality_name)
            admin_level_1_names.append(admin_level_1_name)
            country_names.append(country_name)
            locality_ids.append(locality.get("id"))
            admin_level_1_ids.append(admin_level_1.get("id"))
            country_ids.append(country.get("id"))
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

        new_places = [] # combine areas and extrated location together in this list

        # format areas
        for place in location["areas"]:

            cleaned_place = clean_token(place)
            
            if cleaned_place != "" and cleaned_place != location["country"] and \
               cleaned_place not in new_places:
                new_places.append(cleaned_place)


        # format extracted field
        if record["extracted_location"]:

            cleaned_place = clean_token(record["extracted_location"])

            if cleaned_place != "" and cleaned_place != location["country"] and \
               cleaned_place not in new_places:
                new_places.append(cleaned_place)

        # take edge cases into account
        exception_mappings = {
            "west siberia": "siberian federal district",
            "east siberia": "siberian federal district",
        }
        new_places = [
            exception_mappings[token] if token in exception_mappings else token for token in new_places
        ]

        location["areas"] = new_places

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

    def _search_tokens_diff_order(self, areas, second):
        """
        This method uses the geocoder to make a query of areas (a list of tokens) and second (a string).
        we vary the order of the tokens in areas as they could be wrong. We also remove them if we still don't have a result
        it tried all permutations up to the point where it doesn't include tokens from areas anymore (only "second")
        """

        for l in range(len(areas), -1, -1):
            for option in itertools.permutations(areas, l):

                query = ", ".join(option) + ", " + second
                locality = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                if locality:
                    return locality

        return None

        
    def _find_full_locality(self, areas, country, i, localities, localy_osm_ids, admin_level_1s, admin_level_1_ids):

        # we must use an unstructured search as we have no idea what the tokens could be
        locality = self._search_tokens_diff_order(areas[::-1], country.get("name"))
        if not locality:
            return

        address = locality.get("address")
        locality_name = self._geocoder.get_locality_name(address)
        if locality_name is not None:
            localities[i] = locality_name
            localy_osm_ids[i] = self._geocoder.create_feature_id(locality.get("osm_type"), locality.get("osm_id")) 
        
        admin_level_1_name = self._geocoder.get_admin_level_1_name(address)
        query = f"{admin_level_1_name}, {country.get("name")}"
        admin_level_1 = self._geocoder.get_feature(
            "search", {"query": query}, term=query, term_type="query"
        )
        admin_level_1s[i] = admin_level_1_name
        admin_level_1_ids[i] = admin_level_1.get("id")

    def _geocode(self):
        """Geocode GenBank data."""

        # extract information from the strain (could be whatever)
        self._data["extracted_location"] = self._data.apply(
            self._get_location_from_strain, axis=1
        )
        # compile all the information we know about the location inside this column
        self._data["location"] = self._data.apply(self._format_location, axis=1)

        # initialise to zero all output columns
        country_names = [ None for _ in range(len(self._data))] 
        country_ids = [ None for _ in range(len(self._data))] 
        localities = [ None for _ in range(len(self._data))] 
        localy_osm_ids = [ None for _ in range(len(self._data))]
        admin_level_1s = [ None for _ in range(len(self._data))]
        admin_level_1_ids = [ None for _ in range(len(self._data))]
        # those two won't be filled in in this dataset
        longitudes = [ None for _ in range(len(self._data))]
        latitudes = [ None for _ in range(len(self._data))]

        admin_levels_table = get_admin_levels_table()

        # now parse each row to find its location
        # use the administrative_units dataset and the Nominatim API
        for i, row in self._data.iterrows():

            location = json.loads(row["location"])

            if location["country"] is None:
                continue
                
            # get country from API to get the osm_id
            api_args = {"query": location["country"]}
            country = self._geocoder.get_feature(
                "search", api_args, term=location["country"], term_type="query"
            )
            country_names[i] = country.get("name")
            country_ids[i] = country.get("id")

            areas = location["areas"] # making a copy as we will modify "areas"
            if len(areas) == 0:
                continue

            # try to match tokens in areas with the administrative_units file
            admin_levels_found = []
            admin_levels_country = []

            if country.get("name") in admin_levels_table:
                admin_levels_country = admin_levels_table[country.get("name")]

                for admin_level in admin_levels_country:
                    if admin_level["name"].lower() in areas:
                        areas.remove(admin_level["name"].lower()) # this removes one occurance on purpose
                        admin_levels_found.append(admin_level)
            
            # we couldn't identify any token, we must use the api to find all fields
            if len(admin_levels_found) == 0:
                self._find_full_locality(areas, country, i, localities, localy_osm_ids, admin_level_1s, admin_level_1_ids)
                continue
            
            # now we check if we already found the highest admin level (hence lowest value) of the location
            min_admin_level_country = min(map(lambda x: x["admin_level"], admin_levels_country))
            if country.get("name") in MIN_ADMIN_EXCEPTIONS:
                min_admin_level_country = MIN_ADMIN_EXCEPTIONS[country.get("name")] 
            my_min_admin_level = min(admin_levels_found, key=lambda x: x["admin_level"])
            
            if min_admin_level_country == my_min_admin_level["admin_level"]:
                admin_level_1s[i] = my_min_admin_level["name"]
                admin_level_1_ids[i] = my_min_admin_level["osm_id"]
                admin_levels_found.remove(my_min_admin_level)

                # now check if you have all the rest of the info you need
                if len(areas) == 0 and len(admin_levels_found) > 0:
                    localities[i] = admin_levels_found[0]["name"]
                    localy_osm_ids[i] = admin_levels_found[0]["osm_id"]
                    continue

            # we got no more information
            if len(areas) == 0 and len(admin_levels_found) == 0:
                continue

            # we are here if we haven't identified the highest admin boundary, or we might still have
            # information on the city that wasn't matched with admin_boundaries

            sorted(admin_levels_found, key=lambda x: x["admin_level"], reverse=True)
            adminLevelsMatched = list(map(lambda x: x["name"], admin_levels_found))
            admin_level = "" if admin_level_1s[i] is None else admin_level_1s[i]
            second = ", ".join(adminLevelsMatched) + ", " + admin_level + ", " + country.get("name")
            locality = self._search_tokens_diff_order(areas[::-1], second)
            if not locality:
                continue

            address = locality.get("address")
            locality_name = self._geocoder.get_locality_name(address)
            if locality_name is not None:
                localities[i] = locality_name
                localy_osm_ids[i] = self._geocoder.create_feature_id(locality.get("osm_type"), locality.get("osm_id")) 
            
            # maybe above you already identified the highest admin boundary
            if admin_level_1s[i] is None:
                admin_level_1_name = self._geocoder.get_admin_level_1_name(address)
                query = f"{admin_level_1_name}, {country.get("name")}"
                admin_level_1 = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                admin_level_1s[i] = admin_level_1_name
                admin_level_1_ids[i] = admin_level_1.get("id")


        self._data["locality"] = localities
        self._data["locality_osm_id"] = localy_osm_ids
        self._data["admin_level_1"] = admin_level_1s
        self._data["admin_level_1_osm_id"] = admin_level_1_ids
        self._data["country"] = country_names
        self._data["country_osm_id"] = country_ids
        self._data["longitude"] = longitudes
        self._data["latitude"] = latitudes

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
        location = record["place_of_infection"]
        if not location:
            location = record["place_of_infection_evd"]
        if not location and record["imported"] == "N":
            location = record["place_of_notification"]
        if not location and record["imported"] == "N":
            location = record["reporting_country"]
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
        country_names = []
        country_ids = []
        for index, row in self._data.iterrows():
            location_match = re.match(r"([A-Z]{2})", str(row["location"]))
            if not location_match:
                country_names.append(None)
                country_ids.append(None)
                continue
            country_code = location_match.group(1)
            api_args = {"query": country_code}
            country = self._geocoder.get_feature(
                "search", api_args, term=country_code, term_type="query"
            )
            country_names.append(country.get("name"))
            country_ids.append(country.get("id"))
        self._data["locality"] = None
        self._data["locality_osm_id"] = None
        self._data["admin_level_1"] = None
        self._data["admin_level_1_osm_id"] = None
        self._data["country"] = country_names
        self._data["country_osm_id"] = country_ids

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
