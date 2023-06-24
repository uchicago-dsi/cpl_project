## Path to Equity: Investigating Transit Times to Chicago Public Library Branches
### CAN Summer Experience (2023)

#### Background
Chicago Public Library (CPL) has provided city residents with “free and open places to gather, learn, connect, read and be transformed” since it first opened its doors in 1893. Today, it boasts 81 locations, 4.5 million annual visitors, one million card holders, and six million items in its collection.

To improve its operations, CPL would like to perform a geospatial analysis to determine whether its resources are equitably accessible to all Chicago residents, especially those from underrepresented communities who rank library services as “very important”: i.e., African-Americans, Hispanics, and adults from households with lower income or educational attainment.

This repository holds the results of an analysis to help CPL identify inequities in branch access. Students averaged travel times to library branches using transportation data and then examined correlations between those travel times and different demographic variables (race, socioeconomic status, preferred transportation mode, Internet access needs, etc.).

#### Directory

**data**: Contains raw CPL library branch hour and location, circulation, computer session, and visitor datasets pulled directly from the Chicago Open Data Portal, as well as cleaned versions of the datasets. NOTE: Data is available and current through April 2023.

**notebooks**: Contains Jupyter notebooks used for the analysis.

- clean_branches.ipynb: Consolidates the monthly time series datasets for library branch metrics into a single file and cleans and saves library branch names and locations as a separate file.

- clean_tracts.ipynb: Creates datasets of demographic data (e.g., population, education, income, Internet usage, and commute patterns) for census tracts within the City of Chicago.
