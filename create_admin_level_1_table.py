"""
This script generates the "admin_level_1" file. 
It uses the "countries" and "administrative_units" files.
"""

import pandas as pd

from epinorm.config import (
    ADMIN_LEVEL_1_FILE,
    COUNTRIES_FILE,
    ADMIN_UNITS_FILE,
    NUTS_2024_FILE,
    ADMIN_LEVEL_1_EXCEPTIONS,
    NUTS_CODE_TO_COUNTRY_EXCEPTIONS
)

def main():
    df = pd.DataFrame(columns=["country_code", "nuts_level", "osm_level"])

    # get all country codes on planet
    countries = pd.read_csv(COUNTRIES_FILE)
    df["country_code"] = countries["alpha_2"].dropna()

    # this is used for reference, we want to know which countries (country code) that have nuts codes
    countries_with_nuts = set(pd.read_csv(NUTS_2024_FILE)["Country code"].dropna())
    countries_with_nuts.update(NUTS_CODE_TO_COUNTRY_EXCEPTIONS.values()) # exceptions

    admin_units = pd.read_table(ADMIN_UNITS_FILE)

    # now for each country code try to match the nuts level with its osm_level in the admin_units file
    for i, row in df.iterrows():

        # exceptions
        if row["country_code"] in ADMIN_LEVEL_1_EXCEPTIONS:
            df.loc[i, "nuts_level"] = ADMIN_LEVEL_1_EXCEPTIONS[row["country_code"]]["nuts_level"]
            df.loc[i, "osm_level"] = ADMIN_LEVEL_1_EXCEPTIONS[row["country_code"]]["osm_level"]
            continue

        admin_units_country = admin_units[admin_units["iso3166_1_code"] == row["country_code"]]
        if len(admin_units_country) == 0:
            # we don't know anything about this country (should be small insignificant countries)
            continue

        # it is a EU country
        if row["country_code"] in countries_with_nuts:
            df.loc[i, "nuts_level"] = 3 # default value

            rows_with_nuts_code = admin_units_country[admin_units_country["nuts_code"].notna()]
            rows_with_nuts_code_level = rows_with_nuts_code[rows_with_nuts_code["nuts_code"].apply( \
                                                                    lambda nuts_code: len(nuts_code) == 5)]
            if len(rows_with_nuts_code_level) == 0: # doesn't have nuts code of that length in the file
                continue

            # we want the most precise admin_level if there are multiple
            df.loc[i, "osm_level"] = max(rows_with_nuts_code_level["admin_level"])
        
        # it is a non-EU country
        else:

            # we use the less precise admin level (closest to the country)
            df.loc[i, "osm_level"] = min(admin_units_country["admin_level"])

    # save file to disk
    df.to_csv(ADMIN_LEVEL_1_FILE, index=False)
    print("Successfully created 'admin_level_1.csv' file")


if __name__ == "__main__":
    main()