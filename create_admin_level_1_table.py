"""
This script generates the "admin_level_1" file for the project.
It uses the "countries" and "administrative_units" files
"""

import pandas as pd
from epinorm.config import (
    ADMIN_LEVEL_1_DATA,
    COUNTRIES_DATA,
    ADMIN_LEVELS_DATA,
    MIN_ADMIN_EXCEPTIONS
)


def main():
    df = pd.DataFrame(columns=["country_code", "NUTS_level", "osm_level"])

    countries = pd.read_csv(COUNTRIES_DATA)
    df["country_code"] = countries["alpha_2"].dropna()

    # now for each country code try to match the nuts level with its osm_level in the admin_units file
    admin_units = pd.read_table(ADMIN_LEVELS_DATA)
    for i, row in df.iterrows():

        country_code = row["country_code"]

        admin_units_country = admin_units[admin_units["iso3166_1_code"] == country_code]
        if len(admin_units_country) == 0: # we don't know anything about this country
            continue

        rows_with_nuts_code = admin_units_country[admin_units_country["nuts_code"].notna()]

        if len(rows_with_nuts_code) == 0: # country doesn't have any nuts code (like non-EU countries)
            df.loc[i, "osm_level"] = min(admin_units_country["admin_level"])

            if country_code in MIN_ADMIN_EXCEPTIONS:
                df.loc[i, "osm_level"] = MIN_ADMIN_EXCEPTIONS[country_code]
                
            continue

        # try nuts 3 and then go down until you find a mapping
        for nuts_level in [3, 2, 1]:

            admin_units_nuts3 = rows_with_nuts_code[rows_with_nuts_code["nuts_code"].apply(lambda nut_code: len(nut_code) == nuts_level + 2)]
            if len(admin_units_nuts3) == 0: # doesn't have nuts code of that length
                continue

            df.loc[i, "NUTS_level"] = nuts_level
            df.loc[i, "osm_level"] = max(admin_units_nuts3["admin_level"])
            break

    # save file to disk
    df.to_csv(ADMIN_LEVEL_1_DATA, index=False)


if __name__ == "__main__":
    main()