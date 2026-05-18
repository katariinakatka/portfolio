# autodata.py

This script preprocesses the Finnish Vehicle Register open data provided by Traficom
and stores the result in a SQLite database.

## Description

The script reads `TieliikenneAvoinData_31_12_2025.csv`, filters and cleans the data
to include only privately used passenger cars (classes M1 and M1G), and writes the
processed dataset to `autodata.db`.

## Preprocessing steps

- Filters rows by vehicle class, usage type, and body type
- Keeps only 13 relevant columns
- Replaces municipality codes with names using `kuntarajat.json`
- Converts date columns to datetime format
- Sets CO2 emissions to 0 for all electric vehicles
- Standardizes car brand names in the `merkkiSelvakielinen` column

## Requirements

- Python 3
- pandas
- sqlite3 (standard library)

## Usage

Place `TieliikenneAvoinData_31_12_2025.csv` and `kuntarajat.json` in the same
directory as the script, then run:

    python autodata.py

Output: `autodata.db`
