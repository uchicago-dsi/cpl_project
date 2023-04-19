# -------------------------------------------------------------------- #
# ------ Retrieve data from the American Community Survey (ACS) ------ #
# -------------------------------------------------------------------- #

# This code extracts predefined sociodemographic data from the ACS for the
# state of Illinois. A class is created with three atributes and four methods
# in order to get the data and export it to a location specified in DIRECTORY.

import requests
import pandas as pd

API_key = "c5c557882564fa5b35f0bb0718fe615a8db631ff"
DIRECTORY = "../data/"


class CensusAPI:
    """
    Extracts data from the US Census Data.
    """

    def __init__(self, census_key, year):
        """
        Initializes a new instance of the CensusAPI class.

        Inputs:
            - census_key (str): A string representing the API key
              needed to access the US Census Bureau API
            - year (int): An integer with the year the data want to be
              consulted
        """

        assert isinstance(year, int), f"year parameter is 'str' and should be 'int'"

        self.census_key = census_key
        self.base_url_macro_table = (
            "https://api.census.gov/data/" + str(year) + "/acs/acs5"
        )
        self.base_url_profile_table = (
            "https://api.census.gov/data/" + str(year) + "/acs/acs5/profile"
        )

    def get_data(self, geo, state):
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
            "B01001_029E",
            "B01001_030E",
            "B01001_031E",
            "B01001_032E",
            "B01001_033E",
            "B01001_034E",
            "B01001_035E",
            "B01001_036E",
            "B01001_037E",
            "B01001_038E",
            "B18135_023E",
            "B18135_022E",
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
            "B19058_002E",
            "B19058_003E",
            "B09010_001E",
            "B01003_001E",
            "B09010_002E",
            "B19013_001E",
            "B19123_001E",
            "B19123_002E",
            "B19123_005E",
            "B19123_008E",
            "B19123_011E",
            "B19123_014E",
            "B19123_017E",
            "B19123_020E",
            "DP04_0142PE",
            "DP04_0141PE",
            "DP04_0140PE",
            "DP04_0139PE",
            "DP04_0138PE",
            "DP04_0137PE",
        ]

        # Identify columns that are found in the profile and detailed tables
        profile_columns, macro_columns = self.classify_columns(cols)

        # Define the API calls
        # For the detailed table:
        full_url_macro = f"{self.base_url_macro_table}?get={macro_columns}&for={geo}&in=state:{state}&key={self.census_key}"
        data_response_macro = requests.get(full_url_macro)

        # For the profile tables:
        full_url_profile = f"{self.base_url_profile_table}?get={profile_columns}&for={geo}&in=state:{state}&key={self.census_key}"
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
                "B01001_029E": "total_female_10_to_14",
                "B01001_030E": "total_female_15_to_17",
                "B01001_031E": "total_female_18_to_19",
                "B01001_032E": "total_female_20",
                "B01001_033E": "total_female_21",
                "B01001_034E": "total_female_22_to_24",
                "B01001_035E": "total_female_25_to_29",
                "B01001_036E": "total_female_30_to_34",
                "B01001_037E": "total_female_35_to_39",
                "B01001_038E": "total_female_40_to_44",
                "B18135_023E": "total_19_to_64_no_health_insurance",
                "B18135_022E": "total_19_to_64_public_health_insurance",
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
                "B19058_002E": "total_receives_stamps_snap",
                "B19058_003E": "no_stamps_snap",
                "B09010_001E": "receipt_stamps_snap",
                "B09010_002E": "receipt_stamps_snap_household",
                "B19123_001E": "total_assistance",
                "B19013_001E": "median_income",
                "B01003_001E": "total_pop_in_tract",
                "B19123_002E": "fam_1_with_snap",
                "B19123_005E": "fam_2_with_snap",
                "B19123_008E": "fam_3_with_snap",
                "B19123_011E": "fam_4_with_snap",
                "B19123_014E": "fam_5_with_snap",
                "B19123_017E": "fam_6_with_snap",
                "B19123_020E": "fam_7_with_snap",
                "DP04_0142PE": "rent_percent_35_more",
                "DP04_0141PE": "rent_percent_30_34_9",
                "DP04_0140PE": "rent_percent_25_29_9",
                "DP04_0139PE": "rent_percent_20_24_9",
                "DP04_0138PE": "rent_percent_15_19_9",
                "DP04_0137PE": "rent_percent_15_less",
            }
        )

        merged_df = merged_df.astype(
            dtype={
                "total_population": "int64",
                "total_female": "int64",
                "total_female_10_to_14": "int64",
                "total_female_15_to_17": "int64",
                "total_female_18_to_19": "int64",
                "total_female_20": "int64",
                "total_female_21": "int64",
                "total_female_22_to_24": "int64",
                "total_female_25_to_29": "int64",
                "total_female_30_to_34": "int64",
                "total_female_35_to_39": "int64",
                "total_female_40_to_44": "int64",
                "total_19_to_64_no_health_insurance": "int64",
                "total_19_to_64_public_health_insurance": "int64",
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
                "total_receives_stamps_snap": "int64",
                "no_stamps_snap": "int64",
                "receipt_stamps_snap": "int64",
                "receipt_stamps_snap_household": "int64",
                "total_assistance": "int64",
                "median_income": "int64",
                "fam_1_with_snap": "int64",
                "fam_2_with_snap": "int64",
                "fam_3_with_snap": "int64",
                "fam_4_with_snap": "int64",
                "fam_5_with_snap": "int64",
                "fam_6_with_snap": "int64",
                "fam_7_with_snap": "int64",
                "total_pop_in_tract": "int64",
            }
        )
        merged_df["total_female_mentrual_age"] = merged_df[
            [
                "total_female_10_to_14",
                "total_female_15_to_17",
                "total_female_18_to_19",
                "total_female_20",
                "total_female_21",
                "total_female_22_to_24",
                "total_female_25_to_29",
                "total_female_30_to_34",
                "total_female_35_to_39",
                "total_female_40_to_44",
            ]
        ].apply(sum, axis=1)

        merged_df = merged_df.assign(
            percentage_female_menstrual_age=(
                merged_df.total_female_mentrual_age / merged_df.total_female
            )
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
        export_as = DIRECTORY + "Census_Cook_County_dta.json"
        print("The data was exported to this location:", export_as)
        dataframe.to_json(export_as, orient="records")


# Define the parameter to call on the API
geo = "tract:*"
state = "17"
year = 2021
api = CensusAPI(API_key, year)

merged_df = api.get_data(geo, state)

# Export dataframe
api.export_dataframe_to_json(merged_df)


# def pull_acs_data():
#     """
#     Retrieves 2019 data of the American Census Survey
#         from the US Census API

#     Input: None

#     Returns: None, writes the pulled data as a csv file in the provided path at
#             zip code level demographics for Illinois.
#     """

#     acs_var = censusdata.download('acs5/subject', 2019, censusdata.censusgeo(
#         [('zip%20code%20tabulation%20area', '*')]),
#         ['S0601_C01_047E', 'S1901_C01_013E', 'S1501_C02_009E',
#          'S1501_C02_012E', 'S2301_C04_001E', 'S0101_C01_001E',
#          'S0101_C01_002E', 'S0101_C01_003E', 'S0101_C01_004E',
#          'S0101_C01_005E', 'S0101_C01_006E', 'S0101_C01_007E',
#          'S0101_C01_008E', 'S0101_C01_009E', 'S0101_C01_010E',
#          'S0101_C01_011E', 'S0101_C01_012E', 'S0101_C01_013E',
#          'S0101_C01_014E', 'S0101_C01_015E', 'S0101_C01_016E',
#          'S0101_C01_017E', 'S0101_C01_018E', 'S0101_C01_019E',
#          'S0601_C01_014E', 'S0601_C01_015E', 'S0601_C01_016E',
#          'S0601_C01_017E', 'S0601_C01_021E', 'STATE',
#          'ZCTA'])

#     acs_var = acs_var[acs_var.STATE == 17]
#     acs_var = acs_var.drop('STATE', axis=1)

#     acs_var.columns = ['hh_median_income', 'hh_mean_income', 'perc_educ_highschool',
#                         'perc_educ_bachelor', 'unemployment_rate', 'total_population',
#                         'age_under_5', 'age_5_to_9', 'age_10_to_14',
#                         'age_15_to_19', 'age_20_to_24', 'age_25_to_29',
#                         'age_30_to_34', 'age_35_to_39', 'age_40_to_44',
#                         'age_45_to_49', 'age_50_to_54', 'age_55_to_59',
#                         'age_60_to_64', 'age_65_to_69', 'age_70_to_74',
#                         'age_75_to_79', 'age_80_to_84', 'age_85_up',
#                         'pop_white', 'pop_black', 'pop_native',
#                         'pop_asian', 'pop_latino', 'zip_code']

#     acs_var.to_csv('deprivation_evictions/data_bases/raw_data/acs_data.csv')
