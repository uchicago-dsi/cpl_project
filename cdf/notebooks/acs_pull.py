# -------------------------------------------------------------------- #
# ------ Retrieve data from the American Community Survey (ACS) ------ #
# -------------------------------------------------------------------- #

# This code extracts predefined sociodemographic data from the ACS for the
# state of Illinois and Cook County. A class is created with two atributes and
# four methods in order to get the data and export it to a location specified in DIRECTORY.

# IMPORTANT: An API call can only handle up to 50 variable at the same time.

import requests
import pandas as pd
import numpy as np
from ..constants import API_KEY

DIRECTORY = "./cpl_project/cdf/data/"


class CensusAPI:
    """
    Extracts data from the US Census Data for a specified geographic location and state.
    The default is at the census tract level and for the state of Illinois.
    """

    def __init__(self, year):
        """
        Initializes a new instance of the CensusAPI class.

        Inputs:
            - year (int): An integer with the year the data want to be
              consulted
        """

        assert isinstance(year, int), f"year parameter is 'str' and should be 'int'"

        self.base_url_macro_table = (
            "https://api.census.gov/data/" + str(year) + "/acs/acs5"
        )
        self.base_url_profile_table = (
            "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile"
        )

    def get_data(self, geo="tract:*", state="17"):
        """
        This method extracts data from the US Census Bureau API
        for the specified geographic location and state.

        Inputs:
            - geo (str): A string representing the geographic location
              to retrieve data for.
            - state (str): A string representing the state
              to retrieve data for.

        Returns:
            - dataframe (Pandas df): A DataFrame containing
              the data retrieved from the US Census Bureau API.
        """

        # First letter: Table
        # First two #: subject identifier
        # Three following #: table # within a subject
        # Three digits after _: line number within a table
        # Last letter: E for estimate, M for margin, etc.
        cols = [
            "GEO_ID",
            "NAME",
            "DP05_0001E",
            "DP05_0002E",
            "DP05_0003E",
            "DP05_0005E",
            "DP05_0018E",
            "DP05_0021E",
            "DP05_0024E",
            "DP05_0037E",
            "DP05_0038E",
            "DP05_0039E",
            "DP05_0044E",
            "DP05_0052E",
            "DP05_0057E",
            "DP05_0058E",
            "DP05_0071E",
            "DP05_0086E",
            "DP02_0001E",
            "DP02_0014PE",
            "DP02_0016E",
            "DP02_0017E",
            "DP02_0018E",
            "DP02_0022E",
            "DP02_0053E",
            "DP02_0068E",
            "DP02_0089E",
            "DP02_0094E",
            "DP02_0113E",
            "DP02_0115E",
            "DP02_0152E",
            "DP02_0153E",
            "DP02_0154E",
            "DP03_0009PE",
            "DP03_0024E",
            "DP03_0062E",
            "DP03_0063E",
            "DP03_0066E",
            "DP03_0074E",
            "DP03_0088E",
            "DP03_0119PE",
            "DP03_0128PE",
            "DP04_0002E",
            "DP04_0046E",
            "DP04_0058E",
            "DP04_0101E",
            "DP04_0109E",
            "DP04_0115E",
            "DP04_0124E",
            "DP04_0134E",
        ]

        # Identify columns that are found in the profile and detailed tables
        profile_columns, macro_columns = self.classify_columns(cols)

        # Define the API calls
        # For the detailed table:
        full_url_macro = f"{self.base_url_macro_table}?get={macro_columns}&for={geo}&in=state:{state}&key={API_KEY}"
        data_response_macro = requests.get(full_url_macro)

        # For the profile tables:
        full_url_profile = f"{self.base_url_profile_table}?get={profile_columns}&for={geo}&in=state:{state}&key={API_KEY}"
        data_response_profile = requests.get(full_url_profile)

        macro_json = data_response_macro.json()
        profile_json = data_response_profile.json()

        # Convert JSON data to Pandas dataframes
        macro_df = pd.DataFrame(macro_json[1:], columns=macro_json[0])
        profile_df = pd.DataFrame(profile_json[1:], columns=profile_json[0])

        # Merge dataframes on county and tract
        merged_df = pd.merge(macro_df, profile_df, on=["county", "tract", "state"])

        merged_df = merged_df.rename(
            columns={
                "GEO_ID": "geo_id",
                "NAME": "census_name",
                "DP05_0001E": "total_population",
                "DP05_0002E": "total_male",
                "DP05_0003E": "total_female",
                "DP05_0005E": "under_5_years",
                "DP05_0018E": "median_age",
                "DP05_0021E": "over_18_years",
                "DP05_0024E": "over_65_years",
                "DP05_0037E": "white",
                "DP05_0038E": "black",
                "DP05_0039E": "native",
                "DP05_0044E": "asian",
                "DP05_0052E": "islander",
                "DP05_0057E": "other_race",
                "DP05_0058E": "more_one_race",
                "DP05_0071E": "latino",
                "DP05_0086E": "housing_units_dp5",
                "DP02_0001E": "num_households_dp2",
                "DP02_0014PE": "perc_hh_w_children",
                "DP02_0016E": "avg_household_size",
                "DP02_0017E": "avg_family_size",
                "DP02_0018E": "pop_in_households",
                "DP02_0022E": "pop_in_households_child",
                "DP02_0053E": "pop_enrolled",
                "DP02_0068E": "college_graduates",
                "DP02_0089E": "tot_pop_birth_native",
                "DP02_0094E": "tot_pop_birth_foreign",
                "DP02_0113E": "speaks_english_only",
                "DP02_0115E": "speaks_english_less_very_well",
                "DP02_0152E": "comp_tot_households",
                "DP02_0153E": "comp_tot_households_pc",
                "DP02_0154E": "comp_tot_households_internet",
                "DP03_0009PE": "unemployment_rate",
                "DP03_0024E": "work_from_home",
                "DP03_0062E": "median_hh_income_d",
                "DP03_0063E": "average_hh_income_d",
                "DP03_0066E": "hh_social_security",
                "DP03_0074E": "hh_foodstamps_snap",
                "DP03_0088E": "per_capita_income_d",
                "DP03_0119PE": "perc_fam_bellow_poverty",
                "DP03_0128PE": "perc_ppl_bellow_poverty",
                "DP04_0002E": "occupied_units",
                "DP04_0046E": "occupied_units_owner",
                "DP04_0058E": "occupied_units_no_vehicle",
                "DP04_0101E": "median_owner_cost_unit_mortgage_d",
                "DP04_0109E": "median_owner_cost_unit_no_mortgage_d",
                "DP04_0115E": "rent_burdened_unit_mortgage",
                "DP04_0124E": "rent_burdened_unit_no_mortgage",
                "DP04_0134E": "occupied_median_rent_d",
            }
        )

        return self.move_key_columns_to_front(merged_df)

    def classify_columns(self, column_lst):
        """
        This function takes a list of column names from the US Census,
        and classifies the columns in those belonging to the profile tables
        of the general Census macro table.

        Inputs:
            - column_lst (lst): A list of column names

        Returns:
            - A tuple with a list of profile column and macro columns
        """
        profile_columns = []
        macro_columns = []

        for column in column_lst:
            if column.startswith("DP"):
                profile_columns.append(column)
            else:
                macro_columns.append(column)

        return (",".join(profile_columns), ",".join(macro_columns))

    def move_key_columns_to_front(self, dataframe):
        """
        This function moves the geo columns to the front of the table
        to improve readability

        Inputs:
            - dataframe: a Pandas df

        Returns:
            - a dataframe with re-ordered columns
        """
        cols_to_move = ["tract", "county"]
        dataframe = dataframe[
            cols_to_move + [col for col in dataframe.columns if col not in cols_to_move]
        ]

        return dataframe

    def export_dataframe_to_json(self, dataframe):
        """
        This function exports a Pandas dataframe to a JSON file.

        Inputs:
            dataframe: The dataframe to export
        """
        # Construct the full path to the file
        # export_as = DIRECTORY + "Census_Cook_County_dta.json"
        export_as = "census_cook_county_dta.json"
        # print("The data was exported to this location:", export_as)
        dataframe.to_json(export_as, orient="records")


# Define the parameter to call on the API

year = 2021
api = CensusAPI(year)

merged_df = api.get_data()
merged_df.to_csv(DIRECTORY + "census_cook_county_dta.csv", index=False)

# Export dataframe as JSON
# api.export_dataframe_to_json(merged_df)

