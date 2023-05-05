# -------------------------------------------------------------------- #
# ------ Retrieve data from the American Community Survey (ACS) ------ #
# -------------------------------------------------------------------- #

# This code extracts predefined sociodemographic data from the ACS for the
# state of Illinois. A class is created with three atributes and four methods
# in order to get the data and export it to a location specified in DIRECTORY.

import requests
import pandas as pd
from . import API_KEY

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
        # Last letter: E for estimate.
        cols = [
            "GEO_ID",
            "NAME",
            "B01001_001E",
            "B01001_026E",
            "B19001_002E",
            "B19001_003E",
            "B19001_004E",
            "B19001_005E",
            "B19001_006E",
            "B19001_007E",
            "B19001_008E",
            "B19001_009E",
            "B19001_010E",
            "B19001_011E",
            "B01003_001E",
            "B19013_001E",
            "DP02_0001E",
            "DP02_0017E",
            "DP02_0018E",
            "DP02_0022E",
            "DP02_0053E",
            "DP02_0054E",
            "DP02_0055E",
            "DP02_0056E",
            "DP02_0057E",
            "DP02_0058E",
            "DP02_0069E",
            "DP02_0070E",
            "DP02_0088E",
            "DP02_0089E",
            "DP02_0090E",
            "DP02_0094E",
            "DP02_0095E",
            "DP02_0096E",
            "DP02_0097E",
            "DP02_0152E",
            "DP02_0153E",
            "DP02_0154E",
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
        merged_df = pd.merge(macro_df, profile_df, on=["county", "tract"])

        merged_df = merged_df.rename(
            columns={
                "GEO_ID": "geo_id",
                "NAME": "census_name",
                "B01001_001E": "total_population",
                "B01001_026E": "total_female",
                "B19001_002E": "total_no_income",
                "B19001_003E": "total_with_income",
                "B19001_004E": "total_with_income_level1",
                "B19001_005E": "total_with_income_level2",
                "B19001_006E": "total_with_income_level3",
                "B19001_007E": "total_with_income_level4",
                "B19001_008E": "total_with_income_level5",
                "B19001_009E": "total_with_income_level6",
                "B19001_010E": "total_with_income_level7",
                "B19001_011E": "total_with_income_level8",
                "B19013_001E": "median_income",
                "B01003_001E": "total_pop_in_tract",
                "DP02_0001E": "num_households",
                "DP02_0017E": "avg_family_size",
                "DP02_0018E": "pop_in_households",
                "DP02_0022E": "pop_in_households_child",
                "DP02_0053E": "pop_enrolled",
                "DP02_0054E": "pop_enrolled_nursery",
                "DP02_0055E": "pop_enrolled_kinder",
                "DP02_0056E": "pop_enrolled_elementary",
                "DP02_0057E": "pop_enrolled_highschool",
                "DP02_0058E": "pop_enrolled_college_grad",
                "DP02_0069E": "pop_enrolled_",
                "DP02_0070E": "civilian_pop",
                "DP02_0088E": "tot_pop_birth",
                "DP02_0089E": "tot_pop_birth_native",
                "DP02_0090E": "tot_pop_birth_native_us",
                "DP02_0094E": "tot_pop_birth_foreign",
                "DP02_0095E": "foreign_born",
                "DP02_0096E": "foreign_born_us_citizen",
                "DP02_0097E": "foreign_born_non_us_citizen",
                "DP02_0152E": "comp_tot_households",
                "DP02_0153E": "comp_tot_households_pc",
                "DP02_0154E": "comp_tot_households_internet",
            }
        )

        # Define some variable types:

        merged_df = merged_df.astype(
            dtype={
                "total_population": "int64",
                "total_female": "int64",
                "total_no_income": "int64",
                "total_with_income": "int64",
                "total_with_income_level1": "int64",
                "total_with_income_level2": "int64",
                "total_with_income_level3": "int64",
                "total_with_income_level4": "int64",
                "total_with_income_level5": "int64",
                "total_with_income_level6": "int64",
                "total_with_income_level7": "int64",
                "total_with_income_level8": "int64",
                "median_income": "int64",
                "total_pop_in_tract": "int64",
            }
        )

        # Create extra variables:
        merged_df.loc[:, "total_male"] = (
            merged_df.loc[:, "total_population"] - merged_df.loc[:, "total_female"]
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
# merged_df.to_csv("census_cook_county_dta.csv")

# Export dataframe as JSON
api.export_dataframe_to_json(merged_df)
