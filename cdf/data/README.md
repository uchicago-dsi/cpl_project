# Data Sources
We used three main data sources for this project:

    1. Libraries location: csv file that has the geolocation of each of the 81 branches of Chicago Public Library (CPL) across the city. Provided by CPL.

    2. Census tract boundaries: geojson file that has the corresponding boundaries for each one of the 866 census tracts in the city of Chicago.

    3. American Community Survey (ACS): json file obtained from an API call (See **notebooks/acs_pull.py** ) to the Census Bureau with several demographic variables to define each census tract characteristics. Variables selected by CPL.

Files:
**library_locations** - csv provided to us from CPL partner with all the information for each library branch

**census_cook_county_dta.json** - census api call output from notebooks/acs_pull.py. Need API key to run

**census_tract_boundaries.geojson** - census tract boundaries retrieved from github

**census_tract_data.csv** - unaggregated data at the census tract level for all of Cook County including tracts outside of Chicago

**cpl_agg_data.csv** - output for CPL partner for reviewing. Includes demographic data about populations each library branch is serving based on our assignments

**cpl_assignments.html** rendered map of census tract-library assignments

**vars_dict_operations.xlsx** - created a data dictionary to map census variable codes, variable names, and aggregation operations which was also provided to CPL partner


