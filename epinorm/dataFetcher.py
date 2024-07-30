import pandas as pd
from polyglot.text import Text
from transliterate import translit

def getCountryNamesStandardised():
    """
    THis retrieves the country names in the file "countries", as they are in the file
    """

    df = pd.read_csv("epinorm/data/countries.csv")
    names = df["name"]
    return set(names)

def getAdminLevelsOfCountryTable():

    # * create a mapping from country name to its code
    countryToCode = {}

    df = pd.read_csv("epinorm/data/countries.csv")
    for _, row in df.iterrows():
        countryToCode[row["name"]] = row["alpha_2"]

    # there are exceptions
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


    # now map country code to admin levels
    countryCodeToAdminLevels = {}

    df = pd.read_csv("epinorm/data/administrative_units.tsv", sep="\t")
    for _, row in df.iterrows():

            if row["iso3166_1_code"] not in countryCodeToAdminLevels:
                countryCodeToAdminLevels[row["iso3166_1_code"]] = []

            # a row almost always contains an endonym
            if type(row["endonym"]) is not float:
                entry = {"name": row["endonym"], "admin_level":row["admin_level"], "osmId": row["osm_id"]}
                countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

            # try to tranlisterate the endonym
            try:
                trans = translit(row["endonym"], reversed=True)

                # if the original language was russian, then it might have used the character ь which
                # doesn't get transliterated properly (it gets into an apostrophe)
                if "ь" in row["endonym"] and "'" in trans:
                    trans = trans.replace("'", "")

                entry = {"name":trans, "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)
            except:
                pass


            # when row contains exonym (most of the time but not always)
            if type(row["exonym"]) is not float:

                entry = {"name":row["exonym"], "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

                # some words are okay to be replaced, they are modifiers
                if "District" in row["exonym"]:
                    entry = {"name":row["exonym"].replace("District", "Region"), "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                    countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)
                if "Region" in row["exonym"]:
                    entry = {"name":row["exonym"].replace("Region", "District"), "admin_level":row["admin_level"], "osmId":row["osm_id"]}
                    countryCodeToAdminLevels[row["iso3166_1_code"]].append(entry)

    # now map each country name to its admin levels
    countryToAdminLevels = {}
    for countryName in countryToCode:

        if countryToCode[countryName] not in countryCodeToAdminLevels:
            countryToAdminLevels[countryName] = []

        else:
            countryToAdminLevels[countryName] = countryCodeToAdminLevels[countryToCode[countryName]]

    return countryToAdminLevels

