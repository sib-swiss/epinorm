import json
import pandas as pd
import re

from csv import QUOTE_NONE
from epinorm.config import REF_DATA_DIR
from epinorm.geo import NominatimGeocoder


HOST_SPECIES_FILE = REF_DATA_DIR / "ncbi_host_species.csv"
PATHOGEN_SPECIES_FILE = REF_DATA_DIR / "ncbi_pathogen_species.csv"

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
    "country_osm_id",

    "locality",
    "locality_osm_id",

    "admin_level_1",
    "admin_level_1_osm_id",

    "original_record_source",
    "original_record_id",
    "location",
]


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
    
    def _getAdminLevelsTable(self):
        """
        This method retrives the information in the "countries.csv" and "administrative_units.tsv".
        It returns a dictionnary that maps a country name (from countries.csv) to a list of its administrative levels.
        administrative levels are encoded as dictionaries with properties:
        {
            name,
            admin_level,
            osmId
        }

        it also tranliterates information from the administrative_units.tsv file.
        """

        # * create a mapping from country name to its two letter code
        countryToCode = {}

        df = pd.read_csv("epinorm/data/countries.csv")
        for _, row in df.iterrows():
            countryToCode[row["name"]] = row["alpha_2"]

        # there are exceptions, there are multiple ways of writing one country
        countryToCode["Russia"] = "RU"
        countryToCode["Bolivia"] = "BO"
        countryToCode["Bonaire"] = "BQ"
        countryToCode["Bosnia"] = "BA"
        countryToCode["Iran"] = "IR"
        countryToCode["North Korea"] = "KP"
        countryToCode["North-Korea"] = "KP"
        countryToCode["South Korea"] = "KR"
        countryToCode["South-Korea"] = "KR"
        countryToCode["Moldova"] = "MD"
        countryToCode["Netherlands"] = "NL"
        countryToCode["Palestine"] = "PS"
        countryToCode["Taiwan"] = "TW"
        countryToCode["Tanzania"] = "TZ"
        countryToCode["United Kingdom"] = "GB"
        countryToCode["UK"] = "GB"
        countryToCode["United States"] = "US"
        countryToCode["US"] = "US"
        countryToCode["Venezuela"] = "VE"
        countryToCode["Vietnam"] = "VN"

        # * now map country code to its admin levels
        countryCodeToAdminLevels = {}

        df = pd.read_csv("epinorm/data/administrative_units.tsv", sep="\t")
        for _, row in df.iterrows():

                if row["iso3166_1_code"] not in countryCodeToAdminLevels:
                    countryCodeToAdminLevels[row["iso3166_1_code"]] = []

                # a row sometimes doesn't contain endonym
                if type(row["endonym"]) is not float:
                    entry = {"name": row["endonym"], "admin_level":row["admin_level"], "osmId": row["osm_id"]}
                    countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

                # try to tranlisterate the endonym
                try:
                    transliteration = translit(row["endonym"], reversed=True)

                    # if the original language was russian, then it might have used the character ь which
                    # doesn't get transliterate properly (it gets into an apostrophe)
                    if "ь" in row["endonym"] and "'" in transliteration:
                        transliteration = transliteration.replace("'", "")

                    entry = {"name":transliteration, "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                    countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)
                except:
                    pass


                # when row contains exonym (most of the time but not always)
                if type(row["exonym"]) is not float:

                    entry = {"name":row["exonym"], "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                    countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

                    # some words are okay to be replaced, they are modifiers
                    replacements = {
                    "District" : "Region",
                    "Region" : "District",
                    }
                    for first, second in replacements.items():
                        if first in row["exonym"]:
                            entry = {"name":row["exonym"].replace(first, second), "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                            countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

        # * now map each country name to its admin levels by merging the datasets above
        countryToAdminLevels = {}
        for countryName, countryCode in countryToCode.items():
            if countryCode in countryCodeToAdminLevels:
                countryToAdminLevels[countryName] = countryCodeToAdminLevels[countryCode]
            else:
                countryToAdminLevels[countryName] = []

        return countryToAdminLevels




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
            objects = record["original_location"].split(":", 1)

            location["country"] = objects[0]
            location["areas"] = objects[1].split(",") # even if there isn't a comma you get what we need

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

        location = json.loads(record["original_record_location_description"])


        # format country to make comparison easier
        location["country"] = location["country"].strip().lower()

        newPlaces = [] # combine areas and extrated location together in this list

        # format areas
        for place in location["areas"]:
            place = place.replace("_", " ")
            place = re.sub(r"\(.+\)", "", place)
            place = re.sub(r"[^\w\s-]+", "", place)
            place = place.strip()
            place = place.lower() # for easier comparison later

            if place != "" and place != location["country"] and place not in newPlaces:
                newPlaces.append(place)


        # format extracted field
        if record["extracted_location"]:
            place = record["extracted_location"].replace("_", " ")
            place = re.sub(r"\(.+\)", "", place)
            place = re.sub(r"[^\w\s-]+", "", place)
            place = place.strip()
            place = place.lower() # for easier comparison later

            # the extracted field could be empty after format
            if place != "" and place != location["country"] and place not in newPlaces:
                newPlaces.append(place)

        # take edge cases in to account
        exceptionMappings = {
            "west siberia": "siberian federal district",
            "east siberia": "siberian federal district",
        }
        newPlacesAgain = []
        for token in newPlaces:

            if token in exceptionMappings :
                newPlacesAgain.append(exceptionMappings[token])
            else:
                newPlacesAgain.append(token)
            
        location["areas"] = newPlacesAgain

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

        adminLevelsCountriesTable = self._getAdminLevelsTable()

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

            # now analyse the areas
            if len(location["areas"]) == 0:
                continue
            areas = location["areas"]

            # try to match tokens in areas with the administrative_units file
            adminLevelsFound = []
            adminLevelsCountry = []
            
            if country.get("name") in adminLevelsCountriesTable:
                adminLevelsCountry = adminLevelsCountriesTable[country.get("name")]
                for highestAdminLevelMatched in adminLevelsCountry:
                    if highestAdminLevelMatched["name"].lower() in areas:
                        areas.remove(highestAdminLevelMatched["name"].lower()) # this removes one occurance on purpose
                        adminLevelsFound.append(highestAdminLevelMatched)
            
            # we couldn't identify any token, we must use the api to find all fields
            if len(adminLevelsFound) == 0:

                # we must use an unstructured search as we have no idea what the tokens could be
                query = ", ".join(areas[::-1]) + ", " + country.get("name")
                locality = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                
                # we don't consider those localities
                if not locality :

                    if len(areas) == 1:
                        continue

                    # try again in diffrent order for the tokens
                    query = ", ".join(areas) + ", " + country.get("name")
                    locality = self._geocoder.get_feature(
                        "search", {"query": query}, term=query, term_type="query"
                    )
                    
                    # we don't consider those localities
                    if not locality :
                        continue

                address = locality.get("address")
                localityName = self._geocoder.get_locality_name(address)
                if localityName is not None:
                    localities[i] = localityName
                    localy_osm_ids[i] = self._geocoder.create_feature_id(locality.get("osm_type"), locality.get("osm_id")) 
                
                admin_level_1_name = self._geocoder.get_admin_level_1_name(address)
                query = f"{admin_level_1_name}, {country.get("name")}"
                admin_level_1 = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                admin_level_1s[i] = admin_level_1_name
                admin_level_1_ids[i] = admin_level_1.get("id")

                continue
            
            # now we check if we already found the highest admin level (hence lowest value) of the location
            minAdminLevelCountry = min(map(lambda x: x["admin_level"], adminLevelsCountry))
            if country.get("name") in ["France", "China"]: # exceptions
                minAdminLevelCountry = 4 
            myMinAdminLevelObject = min(adminLevelsFound, key=lambda x: x["admin_level"])
            
            if minAdminLevelCountry == myMinAdminLevelObject["admin_level"]:
                admin_level_1s[i] = myMinAdminLevelObject["name"]
                admin_level_1_ids[i] = myMinAdminLevelObject["osmId"]
                adminLevelsFound.remove(myMinAdminLevelObject)

                # now check if you have all the rest of the info you need
                if len(areas) == 0 and len(adminLevelsFound) > 0:
                    localities[i] = adminLevelsFound[0]["name"]
                    localy_osm_ids[i] = adminLevelsFound[0]["osmId"]
                    continue

            # we got no more information
            if len(areas) == 0 and len(adminLevelsFound) == 0:
                continue

            # we are here if we haven't identified the highest admin boundary, or we might still have
            # information on the city that wasn't matched with admin_boundaries

            sorted(adminLevelsFound, key=lambda x: x["admin_level"], reverse=True)
            adminLevelsMatched = list(map(lambda x: x["name"], adminLevelsFound))
            highestAdminLevelMatched = "" if admin_level_1s[i] is None else admin_level_1s[i]
            query = ", ".join(areas[::-1] + adminLevelsMatched) + ", " + highestAdminLevelMatched + ", " + country.get("name")
            locality = self._geocoder.get_feature(
                "search", {"query": query}, term=query, term_type="query"
            )
            if not locality:

                if len(areas) == 1:
                    continue

                # try again in diffrent order for the tokens
                query = ", ".join(areas + adminLevelsMatched) + ", " + highestAdminLevelMatched + ", " + country.get("name")
                locality = self._geocoder.get_feature(
                    "search", {"query": query}, term=query, term_type="query"
                )
                
                # we don't consider those localities
                if not locality :
                    continue

            address = locality.get("address")
            localityName = self._geocoder.get_locality_name(address)
            if localityName is not None:
                localities[i] = localityName
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
