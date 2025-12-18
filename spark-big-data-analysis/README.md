# Python and Apache Spark Data Analysis Projects

This project consists of multiple data analysis tasks based on real-world
datasets. The focus is on data loading, cleaning, aggregation, and analytical
problem-solving using Python. The analyses demonstrate skills relevant to
data engineering and analytics, including working with different data formats,
large datasets, and geospatial data.



## Project 1: Video Game Sales Analysis (CSV Data)

### Overview
This analysis explores video game sales data based on a publicly available
Kaggle dataset. The dataset contains global and regional sales figures
(in millions) for video games released across multiple years.

### Objectives
- Identify the publisher with the highest total video game sales in Japan
  for games released between 2001 and 2010.
- Analyze yearly sales trends for this publisher, including:
  - Total sales in Japan
  - Total global sales
  - Total global sales for PlayStation 2 (PS2) games
- Handle missing (NULL) sales values by treating them as zero.

### Key Topics
- CSV data loading
- Data cleaning and null handling
- Grouping and aggregation
- Time-based filtering
- Sales analysis



## Project 2: Building Density Analysis in Finnish Municipalities (Parquet Data)

### Overview
This project analyzes building location data in Finland using a dataset
provided in Parquet format. The goal is to examine building density across
municipalities using postal code areas as distinct geographic units.

### Objectives
- Identify the 10 municipalities with the highest ratio of buildings per area.
- For each municipality, calculate:
  - Number of areas (postal codes)
  - Number of streets
  - Number of buildings
  - Building-per-area ratio
  - Shortest direct distance from any building to the Hervanta campus
    (Kampusareena building)

### Key Topics
- Parquet data processing
- Grouping by geographic regions
- Ratio-based metrics
- Aggregation across hierarchical data
- Efficient handling of large datasets



## Project 3: Closest Building to Average Location (Parquet data from project 2)

### Overview
This analysis focuses on spatial data and distance calculations using building
location data.

Two subsets of buildings are analyzed:
- All buildings in Tampere
- Buildings in the Hervanta area (postal code 33720)

### Objectives
- Calculate average geographic coordinates for each subset.
- Identify the building closest to the average location.
- Determine the address of the closest building.
- Compute the distance between the average location and the selected building
  using the Haversine formula.

### Key Topics
- Geospatial data analysis
- Coordinate averaging
- Distance calculations (Haversine)
- Practical location-based problem solving

## Technologies Used
- Python
- Apache Spark



